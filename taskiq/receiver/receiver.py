import asyncio
import contextvars
import functools
import inspect
import random
import sys
from collections.abc import AsyncGenerator, Callable
from concurrent.futures import Executor, ProcessPoolExecutor
from dataclasses import dataclass
from logging import getLogger
from time import time
from typing import Any, get_type_hints

import anyio
from taskiq_dependencies import DependencyGraph

from taskiq.abc.broker import AckableMessage, AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.acks import AckController, AcknowledgeType, parse_acknowledge_type
from taskiq.context import Context
from taskiq.exceptions import NoResultError
from taskiq.message import TaskiqMessage
from taskiq.receiver.params_parser import parse_params
from taskiq.receiver.runtime import ReceiverRuntimeState
from taskiq.result import TaskiqResult
from taskiq.state import TaskiqState
from taskiq.utils import maybe_awaitable

logger = getLogger(__name__)
PY_VERSION = sys.version_info
QUEUE_DONE = object()


@dataclass(frozen=True, slots=True)
class _PrefetchedMessage:
    """A delivery admitted to the runner with its prefetch-slot ownership."""

    data: bytes | AckableMessage
    owns_prefetch_slot: bool


@dataclass(slots=True)
class _PrefetchState:
    """Own one broker iterator and its pending read task."""

    iterator: AsyncGenerator[bytes | AckableMessage, None]
    current_message: asyncio.Task[bytes | AckableMessage] | None


def _execute_sync_task_in_executor(
    target: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    """Execute a sync task.

    This is a wrapper to ensure we pass the target function directly
    to the executor, avoiding issues with pickling bound methods like ctx.run.

    :param target: function to execute
    :param args: positional arguments
    :param kwargs: keyword arguments
    :return: result of the function call
    """
    return target(*args, **kwargs)


class Receiver:
    """Class that uses as a callback handler."""

    def __init__(
        self,
        broker: AsyncBroker,
        executor: Executor | None = None,
        validate_params: bool = True,
        max_async_tasks: "int | None" = None,
        max_async_tasks_jitter: int = 0,
        max_prefetch: int = 0,
        propagate_exceptions: bool = True,
        run_startup: bool = True,
        ack_type: AcknowledgeType | None = None,
        on_exit: Callable[["Receiver"], None] | None = None,
        max_tasks_to_execute: int | None = None,
        wait_tasks_timeout: float | None = None,
    ) -> None:
        self.broker = broker
        self.executor = executor
        self.run_startup = run_startup
        self.validate_params = validate_params
        self.task_signatures: dict[str, inspect.Signature] = {}
        self.task_hints: dict[str, dict[str, Any]] = {}
        self.dependency_graphs: dict[str, DependencyGraph] = {}
        self.propagate_exceptions = propagate_exceptions
        self.on_exit = on_exit
        self.ack_time = ack_type or AcknowledgeType.WHEN_SAVED
        self.known_tasks: set[str] = set()
        self.max_tasks_to_execute = max_tasks_to_execute
        self.wait_tasks_timeout = wait_tasks_timeout
        self._runtime_state: ReceiverRuntimeState | None = None
        self._listen_error: BaseException | None = None
        self._active_tasks: set[asyncio.Task[Any]] = set()
        for task in self.broker.get_all_tasks().values():
            self._prepare_task(task.task_name, task.original_func)
        self.sem: asyncio.Semaphore | None = None
        if max_async_tasks is not None and max_async_tasks > 0:
            # Apply jitter to prevent all workers from hitting the limit simultaneously
            actual_limit = max_async_tasks
            if max_async_tasks_jitter > 0:
                # Using standard random for load distribution, not cryptography
                actual_limit = max_async_tasks + random.randint(  # noqa: S311
                    0,
                    max_async_tasks_jitter,
                )
            self.sem = asyncio.Semaphore(actual_limit)
        else:
            logger.warning(
                "Setting unlimited number of async tasks "
                "can result in undefined behavior",
            )
        if max_prefetch < 0:
            raise ValueError("max_prefetch cannot be negative.")
        self.sem_prefetch = asyncio.Semaphore(max_prefetch + 1)
        self.is_process_pool = isinstance(executor, ProcessPoolExecutor)

    def attach_runtime_state(
        self,
        runtime_state: ReceiverRuntimeState,
    ) -> ReceiverRuntimeState:
        """Attach process-wide limits supplied by a multi-broker runtime."""
        self._runtime_state = runtime_state
        self.sem = runtime_state.execution_semaphore
        self.sem_prefetch = runtime_state.prefetch_semaphore
        return runtime_state

    def is_runtime_state_attached(
        self,
        runtime_state: ReceiverRuntimeState,
    ) -> bool:
        """Return whether this Receiver uses all supplied process-wide state."""
        return (
            self._runtime_state is runtime_state
            and self.sem is runtime_state.execution_semaphore
            and self.sem_prefetch is runtime_state.prefetch_semaphore
        )

    async def callback(  # noqa: C901, PLR0912
        self,
        message: bytes | AckableMessage,
        raise_err: bool = False,
    ) -> None:
        """
        Receive new message and execute tasks.

        This method is used to process message,
        that came from brokers.

        :raises Exception: if raise_err is true,
            and exception were found while saving result.
        :param message: received message.
        :param raise_err: raise an error if cannot save result in
            result_backend.
        """
        ack_controller = AckController(
            message.ack if isinstance(message, AckableMessage) else None,
        )
        message_data = message.data if isinstance(message, AckableMessage) else message
        try:
            taskiq_msg = self.broker.formatter.loads(message=message_data)
            taskiq_msg.parse_labels()
        except Exception as exc:
            logger.warning(
                "Cannot parse message: %s. Skipping execution.\n %s",
                message_data,
                exc,
                exc_info=True,
            )
            return
        logger.debug(f"Received message: {taskiq_msg}")
        task = self.broker.find_task(taskiq_msg.task_name)
        if task is None:
            logger.warning(
                'task "%s" is not found. Maybe you forgot to import it?',
                taskiq_msg.task_name,
            )
            return
        logger.debug(
            "Function for task %s is resolved. Executing...",
            taskiq_msg.task_name,
        )
        for middleware in self.broker.middlewares:
            if middleware.__class__.pre_execute != TaskiqMiddleware.pre_execute:
                taskiq_msg = await maybe_awaitable(
                    middleware.pre_execute(
                        taskiq_msg,
                    ),
                )

        ack_time = self._get_ack_time(taskiq_msg)
        logger.info(
            "Executing task %s with ID: %s",
            taskiq_msg.task_name,
            taskiq_msg.task_id,
        )

        if ack_time == AcknowledgeType.WHEN_RECEIVED and ack_controller.is_ackable:
            await ack_controller.ack()

        result = await self.run_task(
            target=task.original_func,
            message=taskiq_msg,
            ack_controller=ack_controller,
        )

        if (
            ack_time == AcknowledgeType.WHEN_EXECUTED
            and ack_controller.is_ackable
            and not ack_controller.requeue_failed
        ):
            await ack_controller.ack()

        for middleware in reversed(self.broker.middlewares):
            if middleware.__class__.post_execute != TaskiqMiddleware.post_execute:
                await maybe_awaitable(middleware.post_execute(taskiq_msg, result))

        try:
            if not isinstance(result.error, NoResultError):
                await self.broker.result_backend.set_result(taskiq_msg.task_id, result)

                for middleware in reversed(self.broker.middlewares):
                    if middleware.__class__.post_save != TaskiqMiddleware.post_save:
                        await maybe_awaitable(middleware.post_save(taskiq_msg, result))

        except Exception as exc:
            logger.exception(
                "Can't set result in result backend. Cause: %s",
                exc,
                exc_info=True,
            )
            if raise_err:
                raise exc

        should_ack_saved = (
            ack_time == AcknowledgeType.WHEN_SAVED and not ack_controller.requeue_failed
        )
        should_ack_manual_requeue = (
            ack_time == AcknowledgeType.MANUAL and ack_controller.requeue_published
        )
        if (
            should_ack_saved or should_ack_manual_requeue
        ) and ack_controller.is_ackable:
            await ack_controller.ack()

    def _get_ack_time(self, message: TaskiqMessage) -> AcknowledgeType:
        """
        Get acknowledge time for a task.

        Task-level `ack_type` label overrides worker-level configuration.
        """
        ack_type = message.labels.get("ack_type")
        if ack_type is None:
            return self.ack_time
        try:
            return parse_acknowledge_type(ack_type)
        except ValueError as exc:
            raise ValueError(
                f"Invalid ack_type label {ack_type!r} for task {message.task_name}.",
            ) from exc

    async def run_task(  # noqa: C901, PLR0912, PLR0915
        self,
        target: Callable[..., Any],
        message: TaskiqMessage,
        ack_controller: AckController | None = None,
    ) -> TaskiqResult[Any]:
        """
        This function actually executes functions.

        It has all needed parameters in
        message.

        If the target function is async
        it awaits it, if it's sync
        it wraps it in run_sync and executes in
        threadpool executor.

        Also it uses LogsCollector to
        collect logs.

        :param target: function to execute.
        :param message: received message.
        :return: result of execution.
        """
        loop = asyncio.get_running_loop()
        returned = None
        found_exception: BaseException | None = None
        signature = None
        if message.task_name not in self.known_tasks:
            self._prepare_task(message.task_name, target)
        if self.validate_params:
            signature = self.task_signatures.get(message.task_name)
        dependency_graph = self.dependency_graphs.get(message.task_name)
        parse_params(signature, self.task_hints.get(message.task_name) or {}, message)

        dep_ctx = None
        # Kwargs are defined in another variable,
        # because we want to update them with
        # kwargs resolved by dependency injector.
        kwargs = {}
        if dependency_graph:
            # Create a context for dependency resolving.
            broker_ctx = self.broker.custom_dependency_context
            broker_ctx.update(
                {
                    Context: Context(message, self.broker, ack_controller),
                    TaskiqState: self.broker.state,
                },
            )
            dep_ctx = dependency_graph.async_ctx(
                broker_ctx,
                self.broker.dependency_overrides or None,
            )
            # Resolve all function's dependencies.

        # Start a timer.
        start_time = time()

        check_coroutine_func = (
            asyncio.iscoroutinefunction
            if PY_VERSION <= (3, 13)
            else inspect.iscoroutinefunction
        )
        try:
            # We put kwargs resolving here,
            # to be able to catch any exception (for example ),
            # that happen while resolving dependencies.
            if dep_ctx:
                kwargs = await dep_ctx.resolve_kwargs()
            # We update kwargs with kwargs from network.
            kwargs.update(message.kwargs)
            is_coroutine = True
            # If the function is a coroutine, we await it.
            if check_coroutine_func(target):
                target_future = target(*message.args, **kwargs)
            else:
                is_coroutine = False
                if self.is_process_pool:
                    # For ProcessPoolExecutor, we can't use ctx.run because it contains
                    # a reference to contextvars.Context which cannot be pickled.
                    # Instead, we call the target function directly in the executor.
                    # Each worker process starts with its own context, so we don't need
                    # to preserve the parent context.
                    target_future = loop.run_in_executor(
                        self.executor,
                        _execute_sync_task_in_executor,
                        target,
                        tuple(message.args),
                        kwargs,
                    )
                else:
                    # For ThreadPoolExecutor, we can use ctx.run with functools.partial
                    ctx = contextvars.copy_context()
                    func = functools.partial(target, *message.args, **kwargs)
                    target_future = loop.run_in_executor(
                        self.executor,
                        ctx.run,
                        func,
                    )
            timeout = message.labels.get("timeout")
            if timeout is not None:
                if not is_coroutine:
                    logger.warning("Timeouts for sync tasks don't work in python well.")

                with anyio.fail_after(float(timeout)):
                    target_future = await target_future
                    if inspect.isawaitable(target_future):
                        target_future = await target_future

            else:
                target_future = await target_future
                if inspect.isawaitable(target_future):
                    target_future = await target_future

            returned = target_future
        except NoResultError as no_res_exc:
            found_exception = no_res_exc
            logger.warning(
                "Task %s with id %s skipped setting result.",
                message.task_name,
                message.task_id,
            )
        except BaseException as exc:
            found_exception = exc
            logger.error(
                "Exception found while executing function: %s",
                exc,
                exc_info=True,
            )
        # Stop the timer.
        execution_time = time() - start_time
        if dep_ctx:
            args = (None, None, None)
            if found_exception and self.propagate_exceptions:
                args = (  # type: ignore
                    type(found_exception),
                    found_exception,
                    found_exception.__traceback__,
                )
            await dep_ctx.close(*args)

        # Assemble result.
        result: TaskiqResult[Any] = TaskiqResult(
            is_err=found_exception is not None,
            log=None,
            return_value=returned,
            execution_time=round(execution_time, 2),
            error=found_exception,
            labels=message.labels,
        )
        # If exception is found we execute middlewares.
        if found_exception is not None:
            for middleware in reversed(self.broker.middlewares):
                if middleware.__class__.on_error != TaskiqMiddleware.on_error:
                    await maybe_awaitable(
                        middleware.on_error(
                            message,
                            result,
                            found_exception,
                        ),
                    )

        return result

    async def listen(self, finish_event: asyncio.Event) -> None:  # pragma: no cover
        """
        This function iterates over tasks asynchronously.

        It uses listen() method of an AsyncBroker
        to get new messages from queues.

        Also it has a finish_event, that indicates that
        we need to stop listening for new tasks and shutdown.
        """
        if self.run_startup:
            await self.broker.startup()
        self._listen_error = None
        logger.info("Listening started.")
        queue: asyncio.Queue[_PrefetchedMessage | object] = asyncio.Queue()

        async with anyio.create_task_group() as gr:
            gr.start_soon(self.prefetcher, queue, finish_event)
            gr.start_soon(self.runner, queue)

        if self._listen_error is not None:
            raise self._listen_error

        if self.on_exit is not None:
            self.on_exit(self)

    async def prefetcher(
        self,
        queue: "asyncio.Queue[_PrefetchedMessage | object]",
        finish_event: asyncio.Event,
    ) -> None:
        """
        Prefetch tasks data.

        :param queue: queue for prefetched data.
        :param finish_event: event to indicate that we need to stop prefetching.
        """
        try:
            iterator = await self._open_broker_iterator()
            state = _PrefetchState(
                iterator=iterator,
                current_message=None,
            )
        except BaseException as exc:
            self._record_listen_error(exc)
            await queue.put(QUEUE_DONE)
            return
        fetched_tasks = 0

        try:
            while not self._should_stop_prefetch(finish_event, fetched_tasks):
                message = await self._get_prefetched_message(state, finish_event)
                if message is None:
                    continue
                fetched_tasks += 1
                if self._runtime_state is not None:
                    self._runtime_state.record_fetched_task()
                await queue.put(
                    _PrefetchedMessage(
                        data=message,
                        owns_prefetch_slot=True,
                    ),
                )
                await self._notify_prefetch_hook("on_prefetch_queue_add")
        except StopAsyncIteration:
            pass
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._record_listen_error(exc)
        finally:
            logger.info("Stopping prefetching messages...")
            try:
                await self._enqueue_late_prefetched_message(queue, state)
            finally:
                await queue.put(QUEUE_DONE)

    async def _enqueue_late_prefetched_message(
        self,
        queue: "asyncio.Queue[_PrefetchedMessage | object]",
        state: _PrefetchState,
    ) -> None:
        """Drain a delivery that completed concurrently with listener stop."""
        late_message = await self._close_prefetch_state(state)
        if late_message is None:
            return
        if self._runtime_state is not None:
            self._runtime_state.record_fetched_task()
        await queue.put(
            _PrefetchedMessage(
                data=late_message,
                owns_prefetch_slot=False,
            ),
        )
        try:
            await self._notify_prefetch_hook("on_prefetch_queue_add")
        except BaseException as exc:
            self._record_listen_error(exc)

    async def _open_broker_iterator(
        self,
    ) -> AsyncGenerator[bytes | AckableMessage, None]:
        """Open and validate the transport listener owned by this Receiver."""
        iterator: object = self.broker.listen()
        if inspect.isawaitable(iterator):
            iterator = await iterator
        if not isinstance(iterator, AsyncGenerator):
            raise TypeError("Broker.listen() must return an async generator.")
        return iterator

    def _should_stop_prefetch(
        self,
        finish_event: asyncio.Event,
        fetched_tasks: int,
    ) -> bool:
        """Return whether this Receiver should stop requesting deliveries."""
        if finish_event.is_set():
            return True
        if (
            self._runtime_state is None
            and self.max_tasks_to_execute
            and fetched_tasks >= self.max_tasks_to_execute
        ):
            logger.info("Max number of tasks executed.")
            return True
        return False

    async def _get_prefetched_message(
        self,
        state: _PrefetchState,
        finish_event: asyncio.Event,
    ) -> bytes | AckableMessage | None:
        """Wait briefly for one delivery while holding a prefetch slot."""
        await self.sem_prefetch.acquire()
        if state.current_message is None:
            state.current_message = asyncio.create_task(
                state.iterator.__anext__(),
            )
        current_message = state.current_message
        finish_waiter = asyncio.create_task(finish_event.wait())
        try:
            try:
                done, _ = await asyncio.wait(
                    {current_message, finish_waiter},
                    timeout=0.3,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                finish_waiter.cancel()
                await asyncio.gather(finish_waiter, return_exceptions=True)
        except BaseException:
            self.sem_prefetch.release()
            raise

        if current_message in done:
            state.current_message = None
            try:
                return current_message.result()
            except BaseException:
                self.sem_prefetch.release()
                raise

        if not done:
            self.sem_prefetch.release()
            return None

        self.sem_prefetch.release()
        return None

    async def _close_prefetch_state(
        self,
        state: _PrefetchState,
    ) -> bytes | AckableMessage | None:
        """Close the broker iterator and return a concurrently finished read."""
        late_message: bytes | AckableMessage | None = None
        current_message = state.current_message
        state.current_message = None
        if current_message is not None:
            current_message.cancel()
            try:
                late_message = await current_message
            except (asyncio.CancelledError, StopAsyncIteration):
                pass
            except BaseException as exc:
                self._record_listen_error(exc)

        try:
            await state.iterator.aclose()
        except (asyncio.CancelledError, StopAsyncIteration):
            pass
        except BaseException as exc:
            self._record_listen_error(exc)
        return late_message

    async def _notify_prefetch_hook(self, hook_name: str) -> None:
        """Run one optional prefetch instrumentation hook in reverse order."""
        for middleware in reversed(self.broker.middlewares):
            hook = getattr(middleware, hook_name, None)
            if hook is not None:
                await maybe_awaitable(hook())

    async def runner(
        self,
        queue: "asyncio.Queue[_PrefetchedMessage | object]",
    ) -> None:
        """
        Run tasks.

        :param queue: queue with prefetched data.
        """
        cancellation: asyncio.CancelledError | None = None
        try:
            while True:
                queued_message = await queue.get()
                if queued_message is QUEUE_DONE:
                    break
                if not isinstance(queued_message, _PrefetchedMessage):
                    raise TypeError("Receiver queue contains an invalid message.")
                await self._start_callback(queued_message)

        except asyncio.CancelledError as exc:
            cancellation = exc
        finally:
            with anyio.CancelScope(shield=True):
                cleanup_cancellation = await self._drain_active_tasks(
                    cancel_immediately=cancellation is not None,
                )
            if cancellation is None:
                cancellation = cleanup_cancellation

        if cancellation is not None:
            raise cancellation
        logger.info("The runner is stopped.")

    async def _run_owned_callback(self, message: bytes | AckableMessage) -> None:
        """Run a callback outside repeated listener-scope cancellation."""
        with anyio.CancelScope(shield=True):
            await self.callback(message=message, raise_err=False)

    async def _start_callback(self, message: _PrefetchedMessage) -> None:
        """Transfer one prefetched delivery into an owned callback task."""
        owns_prefetch_slot = message.owns_prefetch_slot
        owns_execution_slot = False
        try:
            await self._notify_prefetch_hook("on_prefetch_queue_remove")
            if self.sem is not None:
                await self.sem.acquire()
                owns_execution_slot = True

            if owns_prefetch_slot:
                self.sem_prefetch.release()
                owns_prefetch_slot = False
            task = asyncio.create_task(
                self._run_owned_callback(message.data),
            )
        except BaseException:
            if owns_prefetch_slot:
                self.sem_prefetch.release()
            if owns_execution_slot and self.sem is not None:
                self.sem.release()
            raise

        self._active_tasks.add(task)

        # Keep a strong reference until completion. Otherwise Python may
        # garbage-collect a detached task while it is still running.
        task.add_done_callback(self._on_callback_done)

    def _on_callback_done(self, task: "asyncio.Task[Any]") -> None:
        """Release shared capacity and retrieve callback failures."""
        self._active_tasks.discard(task)
        if self.sem is not None:
            self.sem.release()
        if task.cancelled():
            return
        task_exception = task.exception()
        if task_exception is not None:
            logger.error(
                "Receiver callback failed outside task execution handling.",
                exc_info=(
                    type(task_exception),
                    task_exception,
                    task_exception.__traceback__,
                ),
            )

    async def _drain_active_tasks(
        self,
        *,
        cancel_immediately: bool,
    ) -> asyncio.CancelledError | None:
        """Wait for owned callbacks and cancel them after the graceful boundary."""
        tasks = set(self._active_tasks)
        if not tasks:
            return None

        logger.info("Waiting for %d running tasks to complete...", len(tasks))
        cancellation: asyncio.CancelledError | None = None
        if cancel_immediately:
            pending = tasks
        else:
            try:
                _, pending = await asyncio.wait(
                    tasks,
                    timeout=self.wait_tasks_timeout,
                )
            except asyncio.CancelledError as exc:
                cancellation = exc
                pending = {task for task in tasks if not task.done()}

        if pending:
            logger.warning(
                "Cancelling %d tasks that exceeded the graceful wait boundary.",
                len(pending),
            )
            cleanup_cancellation = await self._cancel_callback_tasks(pending)
            if cancellation is None:
                cancellation = cleanup_cancellation
        logger.info("No more tasks to wait for. Shutting down.")
        return cancellation

    @staticmethod
    async def _cancel_callback_tasks(
        tasks: set[asyncio.Task[Any]],
    ) -> asyncio.CancelledError | None:
        """Cancel callbacks once and await cleanup despite outer cancellation."""
        for task in tasks:
            task.cancel()
        waiter = asyncio.gather(*tasks, return_exceptions=True)
        cancellation: asyncio.CancelledError | None = None
        while not waiter.done():
            try:
                await asyncio.shield(waiter)
            except asyncio.CancelledError as exc:
                if cancellation is None:
                    cancellation = exc
        return cancellation

    def _record_listen_error(self, error: BaseException) -> None:
        """Preserve the first transport error while allowing iterator cleanup."""
        if self._listen_error is None:
            self._listen_error = error
            return
        logger.error(
            "Additional error while closing broker listener.",
            exc_info=(type(error), error, error.__traceback__),
        )

    def _prepare_task(self, name: str, handler: Callable[..., Any]) -> None:
        """
        Prepare task for execution.

        This function gets function's signature,
        type hints and builds dependency graph.

        It's useful for dynamic dependency resolution,
        because sometimes the receiver can get
        function that is defined in runtime. We need
        to be aware of that.

        :param name: task name.
        :param handler: task handler.
        """
        self.known_tasks.add(name)
        self.task_signatures[name] = inspect.signature(handler)
        self.task_hints[name] = get_type_hints(handler)
        self.dependency_graphs[name] = DependencyGraph(handler)
