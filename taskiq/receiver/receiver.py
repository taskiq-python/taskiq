import asyncio
import contextvars
import functools
import inspect
import random
import sys
from collections.abc import Callable
from concurrent.futures import Executor, ProcessPoolExecutor
from logging import getLogger
from time import time
from typing import Any, get_type_hints

import anyio
from taskiq_dependencies import DependencyGraph

from taskiq.abc.broker import AckableMessage, AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.acks import AcknowledgeType
from taskiq.context import Context
from taskiq.exceptions import NoResultError
from taskiq.message import TaskiqMessage
from taskiq.receiver.batcher import Batcher
from taskiq.receiver.params_parser import parse_params
from taskiq.result import TaskiqResult
from taskiq.state import TaskiqState
from taskiq.utils import maybe_awaitable

logger = getLogger(__name__)
PY_VERSION = sys.version_info
QUEUE_DONE = b"-1"


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
        self.sem_prefetch = asyncio.Semaphore(max_prefetch)
        self.is_process_pool = isinstance(executor, ProcessPoolExecutor)
        self.batcher = Batcher(self._flush_batch)
        self._batch_tasks: set[asyncio.Task[Any]] = set()

    async def _ack_if(
        self,
        message: "bytes | AckableMessage",
        when: AcknowledgeType,
    ) -> None:
        """
        Acknowledge a message if the configured ack time matches.

        :param message: raw or ackable message.
        :param when: ack time this call corresponds to.
        """
        if self.ack_time == when and isinstance(message, AckableMessage):
            await maybe_awaitable(message.ack())

    async def _run_post_execute(
        self,
        message: TaskiqMessage,
        result: "TaskiqResult[Any]",
    ) -> None:
        """
        Run `post_execute` middlewares for a single message.

        :param message: parsed task message.
        :param result: result of the execution.
        """
        for middleware in reversed(self.broker.middlewares):
            if middleware.__class__.post_execute != TaskiqMiddleware.post_execute:
                await maybe_awaitable(middleware.post_execute(message, result))

    async def _save_result(
        self,
        message: TaskiqMessage,
        result: "TaskiqResult[Any]",
        raise_err: bool = False,
    ) -> None:
        """
        Persist a result and run `post_save` middlewares for one message.

        Skips persistence when the task asked for no result. Errors from the
        result backend are logged, and re-raised only when `raise_err` is set.

        :param message: parsed task message.
        :param result: result to store.
        :param raise_err: re-raise result backend errors.
        :raises Exception: if storing fails and `raise_err` is True.
        """
        if isinstance(result.error, NoResultError):
            return
        try:
            await self.broker.result_backend.set_result(message.task_id, result)
            for middleware in reversed(self.broker.middlewares):
                if middleware.__class__.post_save != TaskiqMiddleware.post_save:
                    await maybe_awaitable(middleware.post_save(message, result))
        except Exception as exc:
            logger.exception(
                "Can't set result in result backend. Cause: %s",
                exc,
                exc_info=True,
            )
            if raise_err:
                raise

    async def callback(
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

        logger.info(
            "Executing task %s with ID: %s",
            taskiq_msg.task_name,
            taskiq_msg.task_id,
        )

        await self._ack_if(message, AcknowledgeType.WHEN_RECEIVED)

        result = await self.run_task(
            target=task.original_func,
            message=taskiq_msg,
        )

        await self._ack_if(message, AcknowledgeType.WHEN_EXECUTED)
        await self._run_post_execute(taskiq_msg, result)
        await self._save_result(taskiq_msg, result, raise_err=raise_err)
        await self._ack_if(message, AcknowledgeType.WHEN_SAVED)

    async def batched_callback(  # noqa: C901
        self,
        task_name: str,
        messages: "list[bytes | AckableMessage]",
    ) -> None:
        """
        Execute a batch of accumulated messages as a single call.

        All messages must belong to the same batched task. Each message
        contributes its first positional argument as one element of the list
        passed to the task. The single result (or error) is stored under every
        task_id, and every message is acknowledged according to the configured
        AcknowledgeType.

        :param task_name: name of the batched task.
        :param messages: raw or ackable messages collected for this batch.
        """
        task = self.broker.find_task(task_name)
        if task is None:
            logger.warning(
                'Batched task "%s" is not found. Dropping batch.',
                task_name,
            )
            return

        parsed: list[TaskiqMessage] = []
        ackables: list[AckableMessage] = []
        for message in messages:
            data = message.data if isinstance(message, AckableMessage) else message
            try:
                tmsg = self.broker.formatter.loads(message=data)
                tmsg.parse_labels()
            except Exception as exc:
                logger.warning(
                    "Cannot parse batched message: %s. Skipping.",
                    exc,
                    exc_info=True,
                )
                continue
            parsed.append(tmsg)
            if isinstance(message, AckableMessage):
                ackables.append(message)

        if not parsed:
            return

        for middleware in self.broker.middlewares:
            if middleware.__class__.pre_execute != TaskiqMiddleware.pre_execute:
                for idx, tmsg in enumerate(parsed):
                    # Reassign so a middleware that returns a transformed
                    # message takes effect (same as the non-batched path).
                    parsed[idx] = await maybe_awaitable(
                        middleware.pre_execute(tmsg),
                    )

        for ack in ackables:
            await self._ack_if(ack, AcknowledgeType.WHEN_RECEIVED)

        items = [tmsg.args[0] if tmsg.args else None for tmsg in parsed]
        batch_message = TaskiqMessage(
            task_id=parsed[0].task_id,
            task_name=task_name,
            labels=parsed[0].labels,
            args=[items],
            kwargs={},
        )

        # `on_error` is run per message below, not for the synthetic message.
        result = await self.run_task(
            target=task.original_func,
            message=batch_message,
            run_on_error=False,
        )

        for ack in ackables:
            await self._ack_if(ack, AcknowledgeType.WHEN_EXECUTED)

        for tmsg in parsed:
            await self._finalize_batch_message(tmsg, result)

        for ack in ackables:
            await self._ack_if(ack, AcknowledgeType.WHEN_SAVED)

    async def _finalize_batch_message(
        self,
        message: TaskiqMessage,
        result: "TaskiqResult[Any]",
    ) -> None:
        """
        Run error/post-execute middlewares and store the result for one message.

        Shared by every message in a batch: `on_error` (on real failures),
        `post_execute`, then result persistence with `post_save`.

        :param message: parsed task message.
        :param result: shared batch result.
        """
        if result.error is not None and not isinstance(result.error, NoResultError):
            await self._run_on_error(message, result, result.error)
        await self._run_post_execute(message, result)
        await self._save_result(message, result)

    async def run_task(  # noqa: C901, PLR0912, PLR0915
        self,
        target: Callable[..., Any],
        message: TaskiqMessage,
        run_on_error: bool = True,
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
        :param run_on_error: run `on_error` middlewares here. Batched execution
            sets this to False and runs them per message instead.
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
                    Context: Context(message, self.broker),
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
        if found_exception is not None and run_on_error:
            await self._run_on_error(message, result, found_exception)

        return result

    async def _run_on_error(
        self,
        message: TaskiqMessage,
        result: "TaskiqResult[Any]",
        exception: BaseException,
    ) -> None:
        """
        Run `on_error` middlewares for a single message.

        :param message: parsed task message.
        :param result: result of the failed execution.
        :param exception: exception raised during execution.
        """
        for middleware in reversed(self.broker.middlewares):
            if middleware.__class__.on_error != TaskiqMiddleware.on_error:
                await maybe_awaitable(
                    middleware.on_error(message, result, exception),
                )

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
        logger.info("Listening started.")
        queue: asyncio.Queue[bytes | AckableMessage] = asyncio.Queue()

        async with anyio.create_task_group() as gr:
            gr.start_soon(self.prefetcher, queue, finish_event)
            gr.start_soon(self.runner, queue)

        if self.on_exit is not None:
            self.on_exit(self)

    async def prefetcher(
        self,
        queue: "asyncio.Queue[bytes | AckableMessage]",
        finish_event: asyncio.Event,
    ) -> None:
        """
        Prefetch tasks data.

        :param queue: queue for prefetched data.
        :param finish_event: event to indicate that we need to stop prefetching.
        """
        fetched_tasks: int = 0
        iterator = self.broker.listen()
        current_message: asyncio.Task[bytes | AckableMessage] = asyncio.create_task(
            iterator.__anext__(),  # type: ignore
        )

        while True:
            if finish_event.is_set():
                break
            try:
                await self.sem_prefetch.acquire()
                if (
                    self.max_tasks_to_execute
                    and fetched_tasks >= self.max_tasks_to_execute
                ):
                    logger.info("Max number of tasks executed.")
                    break
                # Here we wait for the message to be fetched,
                # but we make it with timeout so it can be interrupted
                done, _ = await asyncio.wait({current_message}, timeout=0.3)
                # If the message is not fetched, we release the semaphore
                # and continue the loop. So it will check if finished event was set.
                if not done:
                    self.sem_prefetch.release()
                    continue
                # We're done, so now we need to check
                # whether task has returned an error.
                message = current_message.result()
                current_message = asyncio.create_task(iterator.__anext__())  # type: ignore
                fetched_tasks += 1
                await queue.put(message)
                # Custom hooks for OTel and any future instrumentations
                for middleware in reversed(self.broker.middlewares):
                    if hasattr(middleware, "on_prefetch_queue_add"):
                        await maybe_awaitable(
                            middleware.on_prefetch_queue_add(),  # type: ignore
                        )
            except (asyncio.CancelledError, StopAsyncIteration):
                break
        # We don't want to fetch new messages if we are shutting down.
        logger.info("Stopping prefetching messages...")
        current_message.cancel()
        await queue.put(QUEUE_DONE)
        self.sem_prefetch.release()

    def get_batch_config(
        self,
        task_name: str,
    ) -> "tuple[int | None, float | None] | None":
        """
        Return (batch_size, batch_timeout) if the task is batched, else None.

        A task with malformed batch labels is treated as non-batched (returns
        None) and logged, so a single bad message cannot crash the runner.

        :param task_name: name of the task.
        :return: batch config tuple or None for non-batched tasks.
        """
        task = self.broker.find_task(task_name)
        if task is None or not task.labels.get("batch"):
            return None
        size = task.labels.get("batch_size")
        timeout = task.labels.get("batch_timeout")
        try:
            return (
                int(size) if size is not None else None,
                float(timeout) if timeout is not None else None,
            )
        except (TypeError, ValueError):
            logger.warning(
                "Invalid batch config for task %s (size=%r, timeout=%r). "
                "Treating as non-batched.",
                task_name,
                size,
                timeout,
            )
            return None

    def _peek_task_name(self, message: "bytes | AckableMessage") -> str | None:
        """
        Cheaply read the task name from a raw message for batch routing.

        :param message: raw or ackable message.
        :return: task name, or None if it cannot be parsed.
        """
        data = message.data if isinstance(message, AckableMessage) else message
        try:
            return self.broker.formatter.loads(message=data).task_name
        except Exception:
            return None

    async def _flush_batch(
        self,
        task_name: str,
        messages: "list[bytes | AckableMessage]",
    ) -> None:
        """
        Flush handler invoked by the Batcher: run a batch as one task.

        Acquires the semaphore once for the whole batch and releases it when
        the batch task completes.

        :param task_name: name of the batched task.
        :param messages: messages collected for this batch.
        """
        if self.sem is not None:
            await self.sem.acquire()
        task = asyncio.create_task(
            self.batched_callback(task_name=task_name, messages=messages),
        )
        self._batch_tasks.add(task)

        def _done(finished: "asyncio.Task[Any]") -> None:
            self._batch_tasks.discard(finished)
            if self.sem is not None:
                self.sem.release()

        task.add_done_callback(_done)

    async def wait_for_batch_tasks(self) -> None:
        """
        Await all currently running batch executions.

        Used by inplace brokers (e.g. InMemoryBroker) so that tests can wait
        for flushed batches to finish before asserting results.
        """
        for task in list(self._batch_tasks):
            await task

    async def _dispatch_message(
        self,
        message: "bytes | AckableMessage",
        tasks: "set[asyncio.Task[Any]]",
        task_cb: "Callable[[asyncio.Task[Any]], None]",
    ) -> None:
        """
        Route a dequeued message to batching or immediate execution.

        Batched messages are buffered (and flushed later by the Batcher);
        everything else is executed right away via `callback`.

        :param message: message taken from the prefetch queue.
        :param tasks: set tracking in-flight immediate callbacks.
        :param task_cb: done-callback that releases the semaphore for a task.
        """
        batched_name = self._peek_task_name(message)
        cfg = (
            self.get_batch_config(batched_name) if batched_name is not None else None
        )
        if batched_name is not None and cfg is not None:
            size, timeout = cfg
            # Buffering is not execution: release the slot acquired for this
            # dequeue. The batch acquires its own slot at flush time.
            if self.sem is not None:
                self.sem.release()
            await self.batcher.add(batched_name, message, size, timeout)
            return

        task = asyncio.create_task(
            self.callback(message=message, raise_err=False),
        )
        tasks.add(task)

        # We want the task to remove itself from the set when it's done.
        #
        # Because if we won't save it anywhere,
        # python's GC can silently cancel task
        # and this behaviour considered to be a Hisenbug.
        # https://textual.textualize.io/blog/2023/02/11/the-heisenbug-lurking-in-your-async-code/
        task.add_done_callback(task_cb)

    async def runner(
        self,
        queue: "asyncio.Queue[bytes | AckableMessage]",
    ) -> None:
        """
        Run tasks.

        :param queue: queue with prefetched data.
        """
        tasks: set[asyncio.Task[Any]] = set()

        def task_cb(task: "asyncio.Task[Any]") -> None:
            """
            Callback for tasks.

            This function used to remove task
            from the list of active tasks and release
            the semaphore, so other tasks can use it.

            :param task: finished task
            """
            tasks.discard(task)
            if self.sem is not None:
                self.sem.release()

        while True:
            try:
                # Waits for semaphore to be released.
                if self.sem is not None:
                    await self.sem.acquire()

                self.sem_prefetch.release()
                message = await queue.get()
                if message is QUEUE_DONE:
                    await self.batcher.flush_all()
                    all_tasks = tasks | self._batch_tasks
                    if all_tasks:
                        logger.info(
                            "Waiting for %d running tasks to complete...",
                            len(all_tasks),
                        )
                        await asyncio.wait(
                            all_tasks,
                            timeout=self.wait_tasks_timeout,
                        )
                        logger.info("No more tasks to wait for. Shutting down.")
                    break

                # Custom hooks for OTel and any future instrumentations
                for middleware in reversed(self.broker.middlewares):
                    if hasattr(middleware, "on_prefetch_queue_remove"):
                        await maybe_awaitable(
                            middleware.on_prefetch_queue_remove(),  # type: ignore
                        )

                await self._dispatch_message(message, tasks, task_cb)

            except asyncio.CancelledError:
                break
        logger.info("The runner is stopped.")

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
