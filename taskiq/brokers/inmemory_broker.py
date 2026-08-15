import asyncio
from collections import OrderedDict
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, NoReturn, TypeVar

import anyio

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend, TaskiqResult
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.events import TaskiqEvents
from taskiq.exceptions import SendTaskError, UnknownTaskError
from taskiq.message import BrokerMessage
from taskiq.receiver import Receiver

_ReturnType = TypeVar("_ReturnType")


class InmemoryResultBackend(AsyncResultBackend[_ReturnType]):
    """
    Inmemory result backend.

    This resultbackend is intended to be used only
    with inmemory broker.

    It stores all results in a dict in memory.
    """

    def __init__(self, max_stored_results: int = 100) -> None:
        self.max_stored_results = max_stored_results
        self.results: OrderedDict[str, TaskiqResult[_ReturnType]] = OrderedDict()
        self.progress: OrderedDict[str, TaskProgress[Any]] = OrderedDict()

    async def set_result(self, task_id: str, result: TaskiqResult[_ReturnType]) -> None:
        """
        Sets result.

        This method is used to store result of an execution in a
        results dict. But also it removes previous results
        to keep memory footprint as low as possible.

        :param task_id: id of a task.
        :param result: result of an execution.
        """
        if (
            self.max_stored_results != -1
            and len(self.results) >= self.max_stored_results
        ):
            self.results.popitem(last=False)
        self.results[task_id] = result

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Checks whether result is ready.

        Readiness means that result with this task_id is
        present in results dict.

        :param task_id: id of a task to check.
        :return: True if ready.
        """
        return task_id in self.results

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Get result of a task.

        This method is used to get result
        from result dict.

        It throws exception in case if
        result dict doesn't have a value
        for task_id.

        :param task_id: id of a task.
        :param with_logs: this option is ignored.
        :return: result of a task execution.
        """
        return self.results[task_id]

    async def set_progress(
        self,
        task_id: str,
        progress: TaskProgress[Any],
    ) -> None:
        """
        Set progress of task execution.

        :param task_id: task id
        :param progress: task execution progress
        """
        if (
            self.max_stored_results != -1
            and len(self.progress) >= self.max_stored_results
        ):
            self.progress.popitem(last=False)

        self.progress[task_id] = progress

    async def get_progress(
        self,
        task_id: str,
    ) -> TaskProgress[Any] | None:
        """
        Get progress of task execution.

        :param task_id: task id
        :return: progress or None
        """
        return self.progress.get(task_id)


class InMemoryBroker(AsyncBroker):
    """
    This broker is used to execute tasks without sending them elsewhere.

    It's useful for local development, if you don't want to setup real broker.
    """

    def __init__(
        self,
        sync_tasks_pool_size: int = 4,
        max_stored_results: int = 100,
        cast_types: bool = True,
        max_async_tasks: int = 30,
        max_async_tasks_jitter: int = 0,
        propagate_exceptions: bool = True,
        await_inplace: bool = False,
    ) -> None:
        super().__init__()
        self.result_backend: InmemoryResultBackend[Any] = InmemoryResultBackend(
            max_stored_results=max_stored_results,
        )
        self.executor = ThreadPoolExecutor(sync_tasks_pool_size)
        self.receiver = Receiver(
            broker=self,
            executor=self.executor,
            validate_params=cast_types,
            max_async_tasks=max_async_tasks,
            max_async_tasks_jitter=max_async_tasks_jitter,
            propagate_exceptions=propagate_exceptions,
        )
        self.await_inplace = await_inplace
        self._inflight_tasks: dict[
            asyncio.Future[None],
            asyncio.Task[Any] | None,
        ] = {}
        self._running_tasks: set[asyncio.Task[Any]] = set()
        self._running_task_error: BaseException | None = None
        self._accepting_tasks = True
        self._shutdown_lock = asyncio.Lock()
        self._shutdown_complete = False
        self._executor_shutdown_complete = False
        self._pending_shutdown_error: BaseException | None = None
        self._managed_context: ContextVar[asyncio.Future[None] | None] = ContextVar(
            "taskiq_inmemory_managed_context",
            default=None,
        )
        self._deferred_execution: ContextVar[
            asyncio.Future[BrokerMessage | None] | None
        ] = ContextVar("taskiq_inmemory_deferred_execution", default=None)

    @contextmanager
    def _send_lifecycle(self) -> Iterator[None]:
        """Register a client send before it starts using broker resources."""
        try:
            self._ensure_accepting_tasks()
        except Exception as exc:
            raise SendTaskError from exc

        send_done = self._track_inflight_send()
        context_token = self._managed_context.set(send_done)
        try:
            yield
        finally:
            self._managed_context.reset(context_token)
            self._finish_inflight_send(send_done)

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking task.

        This method just executes given task.

        :param message: incoming message.

        :raises TaskiqError: if someone wants to kick unknown task.
        """
        deferred_execution = self._deferred_execution.get()
        if deferred_execution is not None and not deferred_execution.done():
            self._ensure_known_task(message)
            deferred_execution.set_result(message)
            return

        self._ensure_accepting_tasks()
        self._ensure_known_task(message)
        send_done = self._track_inflight_send()
        context_token = self._managed_context.set(send_done)
        try:
            await self._dispatch_message(message)
        finally:
            self._managed_context.reset(context_token)
            self._finish_inflight_send(send_done)

    async def _kick_with_post_send(
        self,
        message: BrokerMessage,
        post_send: Callable[[], Awaitable[None]],
    ) -> None:
        """Run client post-send hooks before local task execution starts."""
        accepted_message, kick_error = await self._accept_message(message)

        if kick_error is not None:
            self._raise_kick_error(accepted_message, kick_error, post_send)

        if accepted_message is None:
            await post_send()
            return

        message = accepted_message

        try:
            await post_send()
        except asyncio.CancelledError:
            self._start_background_task(message)
            raise
        except Exception as post_send_error:
            if not self.await_inplace:
                self._start_background_task(message)
                raise
            await self._finish_after_post_send_error(
                message,
                post_send_error,
            )
        except BaseException:
            self._start_background_task(message)
            raise
        else:
            try:
                await self._dispatch_message(message)
            except Exception as exc:
                raise SendTaskError from exc

    def _raise_kick_error(
        self,
        accepted_message: BrokerMessage | None,
        kick_error: BaseException,
        post_send: Callable[[], Awaitable[None]],
    ) -> NoReturn:
        """Preserve accepted work and expose the public kick failure."""
        if accepted_message is not None:
            self._start_accepted_send(accepted_message, post_send)
        if isinstance(kick_error, Exception):
            raise SendTaskError from kick_error
        raise kick_error

    async def _finish_after_post_send_error(
        self,
        message: BrokerMessage,
        post_send_error: Exception,
    ) -> NoReturn:
        """Finish accepted inline work without replacing its post-send error."""
        task = self._create_managed_task(
            self.receiver.callback(message=message.message),
        )
        try:
            await asyncio.wait((task,))
        except asyncio.CancelledError:
            self._track_running_task(task)
            raise
        try:
            task.result()
        except BaseException as execution_error:
            raise post_send_error from execution_error
        raise post_send_error

    def _ensure_known_task(self, message: BrokerMessage) -> None:
        """Reject direct sends for tasks that are not registered locally."""
        if self.find_task(message.task_name) is None:
            raise UnknownTaskError(task_name=message.task_name)

    def _ensure_accepting_tasks(self) -> None:
        """Reject work once broker shutdown has started."""
        if not self._accepting_tasks:
            raise RuntimeError("InMemoryBroker is shutting down.")

    async def _dispatch_message(self, message: BrokerMessage) -> None:
        """Execute a validated message inline or track it in the background."""
        if self.await_inplace:
            await self.receiver.callback(message=message.message)
            return

        self._start_background_task(message)

    async def _accept_message(
        self,
        message: BrokerMessage,
    ) -> tuple[BrokerMessage | None, BaseException | None]:
        """Run the public send extension point while deferring local execution."""
        deferred_execution = asyncio.get_running_loop().create_future()
        dispatch_token = self._deferred_execution.set(deferred_execution)
        kick_error: BaseException | None = None
        try:
            await self.kick(message)
        except BaseException as exc:
            kick_error = exc
        finally:
            self._deferred_execution.reset(dispatch_token)
            if not deferred_execution.done():
                deferred_execution.set_result(None)
        return deferred_execution.result(), kick_error

    def _start_background_task(self, message: BrokerMessage) -> None:
        """Start and track one local receiver callback."""
        task = self._create_managed_task(
            self.receiver.callback(message=message.message),
        )
        self._track_running_task(task)

    def _start_accepted_send(
        self,
        message: BrokerMessage,
        post_send: Callable[[], Awaitable[None]],
    ) -> None:
        """Finish an accepted send in order after its public override fails."""
        task = self._create_managed_task(
            self._run_accepted_send(message, post_send),
        )
        self._track_running_task(task)

    async def _run_accepted_send(
        self,
        message: BrokerMessage,
        post_send: Callable[[], Awaitable[None]],
    ) -> None:
        """Run post-send and accepted execution while preserving first failure."""
        try:
            await post_send()
        except BaseException as post_send_error:
            try:
                await self.receiver.callback(message=message.message)
            except BaseException as execution_error:
                raise post_send_error from execution_error
            raise
        await self.receiver.callback(message=message.message)

    def _create_managed_task(
        self,
        awaitable: Awaitable[None],
    ) -> asyncio.Task[None]:
        """Create a broker-owned task with a bounded managed context."""
        managed_done = asyncio.get_running_loop().create_future()
        return asyncio.create_task(self._run_managed(awaitable, managed_done))

    async def _run_managed(
        self,
        awaitable: Awaitable[None],
        managed_done: asyncio.Future[None],
    ) -> None:
        """Run one awaitable and release its inherited managed context."""
        context_token = self._managed_context.set(managed_done)
        try:
            await awaitable
        finally:
            self._managed_context.reset(context_token)
            managed_done.set_result(None)

    def _track_running_task(self, task: asyncio.Task[Any]) -> None:
        """Track one local receiver callback until it finishes."""
        self._running_tasks.add(task)
        task.add_done_callback(self._on_running_task_done)

    def _track_inflight_send(self) -> asyncio.Future[None]:
        """Track one accepted send until execution ownership is settled."""
        send_done = asyncio.get_running_loop().create_future()
        self._inflight_tasks[send_done] = asyncio.current_task()
        return send_done

    def _finish_inflight_send(self, send_done: asyncio.Future[None]) -> None:
        """Release an accepted send after execution ownership is settled."""
        self._inflight_tasks.pop(send_done, None)
        if not send_done.done():
            send_done.set_result(None)

    def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Inmemory broker cannot listen.

        This method throws RuntimeError if you call it.
        Because inmemory broker cannot really listen to any of tasks.

        :raises RuntimeError: if this method is called.
        """
        raise RuntimeError("Inmemory brokers cannot listen.")

    async def wait_all(self) -> None:
        """
        Wait for all currently running tasks to complete.

        Useful when used in testing and you need to await all sent tasks
        before asserting results.
        Cancelling this waiter does not cancel accepted executions.
        """
        self._ensure_external_drain("wait_all")

        while self._inflight_tasks or self._running_tasks:
            inflight_sends = tuple(self._inflight_tasks)
            running_tasks = tuple(self._running_tasks)
            await asyncio.gather(
                *(asyncio.shield(task) for task in (*inflight_sends, *running_tasks)),
                return_exceptions=True,
            )
            for send_done in inflight_sends:
                self._inflight_tasks.pop(send_done, None)
            for task in running_tasks:
                self._on_running_task_done(task)

        if self._running_task_error is not None:
            task_error = self._running_task_error
            self._running_task_error = None
            raise task_error

    def _on_running_task_done(self, task: asyncio.Task[Any]) -> None:
        """Release a completed task and retain its first failure until drain."""
        if task not in self._running_tasks:
            return

        self._running_tasks.remove(task)
        try:
            task.result()
        except BaseException as exc:
            if self._running_task_error is None:
                self._running_task_error = exc

    def _ensure_external_drain(self, operation: str) -> None:
        """Reject drain calls made by work that the drain must await."""
        managed_done = self._managed_context.get()
        if managed_done is not None and not managed_done.done():
            raise RuntimeError(
                f"InMemoryBroker.{operation}() cannot be called from "
                "a task managed by this broker.",
            )

        current_task = asyncio.current_task()
        if current_task is None:
            return

        is_managed_execution = (
            current_task in self._running_tasks
            or current_task in self._inflight_tasks
            or current_task in self._inflight_tasks.values()
        )
        if is_managed_execution:
            raise RuntimeError(
                f"InMemoryBroker.{operation}() cannot be called from "
                "a task managed by this broker.",
            )

    def _get_startup_events(self) -> tuple[TaskiqEvents, ...]:
        """Run both sides because tasks execute in the client process."""
        return (TaskiqEvents.CLIENT_STARTUP, TaskiqEvents.WORKER_STARTUP)

    def _get_shutdown_events(self) -> tuple[TaskiqEvents, ...]:
        """Shut down both event phases in their legacy order."""
        return (TaskiqEvents.CLIENT_SHUTDOWN, TaskiqEvents.WORKER_SHUTDOWN)

    async def shutdown(self) -> None:
        """Drain local execution, close resources and stop the executor.

        Cancellation during accepted-work drain is propagated after cleanup.
        """
        self._ensure_external_drain("shutdown")
        self._accepting_tasks = False

        async with self._shutdown_lock:
            if self._shutdown_complete:
                return
            await self._shutdown_once()

    async def _shutdown_once(self) -> None:
        """Run one retryable shutdown attempt while holding the lifecycle lock."""
        shutdown_error = self._pending_shutdown_error
        drain_cancelled = False

        while True:
            try:
                with anyio.CancelScope(shield=drain_cancelled):
                    await self.wait_all()
            except asyncio.CancelledError as exc:
                shutdown_error = self._remember_shutdown_error(
                    shutdown_error,
                    exc,
                )
                drain_cancelled = True
                continue
            except BaseException as exc:
                shutdown_error = self._remember_shutdown_error(
                    shutdown_error,
                    exc,
                )
            break

        shutdown_errors = [] if shutdown_error is None else [shutdown_error]
        drain_cancelled = await self._shutdown_executor(
            shutdown_errors,
            drain_cancelled,
        )

        try:
            with anyio.CancelScope(shield=drain_cancelled):
                await super()._shutdown_resources(shutdown_errors)
        except BaseException:
            if shutdown_errors:
                self._pending_shutdown_error = shutdown_errors[0]
            raise

        self._pending_shutdown_error = None
        self._shutdown_complete = True
        if shutdown_errors:
            raise shutdown_errors[0]

    async def _shutdown_executor(
        self,
        shutdown_errors: list[BaseException],
        drain_cancelled: bool,
    ) -> bool:
        """Close the sync executor without blocking the event loop."""
        if self._executor_shutdown_complete:
            return drain_cancelled

        while True:
            try:
                with anyio.CancelScope(shield=drain_cancelled):
                    await anyio.to_thread.run_sync(
                        self.executor.shutdown,
                        abandon_on_cancel=False,
                    )
            except asyncio.CancelledError as exc:
                self._record_shutdown_error(shutdown_errors, exc)
                drain_cancelled = True
                continue
            except BaseException as exc:  # pragma: no cover
                self._record_shutdown_error(shutdown_errors, exc)
            break

        self._executor_shutdown_complete = True
        return drain_cancelled
