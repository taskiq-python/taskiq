import asyncio
from collections import OrderedDict
from collections.abc import AsyncGenerator, Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

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
            asyncio.Task[Any],
            asyncio.Task[Any] | None,
        ] = {}
        self._running_tasks: set[asyncio.Task[Any]] = set()
        self._running_task_error: BaseException | None = None

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking task.

        This method just executes given task.

        :param message: incoming message.

        :raises TaskiqError: if someone wants to kick unknown task.
        """
        self._ensure_known_task(message)
        await self._dispatch_message(message)

    async def _kick_with_post_send(
        self,
        message: BrokerMessage,
        post_send: Callable[[], Awaitable[None]],
    ) -> None:
        """Run client post-send hooks before local task execution starts."""
        try:
            self._ensure_known_task(message)
        except Exception as exc:
            raise SendTaskError from exc

        execution_task, execution_ready = self._create_gated_execution(message)

        try:
            await post_send()
        except asyncio.CancelledError:
            self._release_to_background(execution_task, execution_ready)
            raise
        except Exception as post_send_error:
            if not self.await_inplace:
                self._release_to_background(execution_task, execution_ready)
                raise
            await self._finish_after_post_send_error(
                execution_task,
                execution_ready,
                post_send_error,
            )
        except BaseException:
            self._release_to_background(execution_task, execution_ready)
            raise

        if not self.await_inplace:
            self._release_to_background(execution_task, execution_ready)
            return

        execution_ready.set()
        try:
            await execution_task
        except Exception as exc:
            raise SendTaskError from exc

    def _create_gated_execution(
        self,
        message: BrokerMessage,
    ) -> tuple[asyncio.Task[Any], asyncio.Event]:
        """Create accepted execution that waits for the send boundary."""
        execution_ready = asyncio.Event()
        execution_task = asyncio.create_task(
            self._execute_after_post_send(message, execution_ready),
        )
        self._track_inflight_task(execution_task)
        return execution_task, execution_ready

    async def _execute_after_post_send(
        self,
        message: BrokerMessage,
        execution_ready: asyncio.Event,
    ) -> None:
        """Wait for post-send completion before executing a local message."""
        await execution_ready.wait()
        await self.receiver.callback(message=message.message)

    async def _finish_after_post_send_error(
        self,
        task: asyncio.Task[Any],
        execution_ready: asyncio.Event,
        post_send_error: Exception,
    ) -> None:
        """Finish accepted inline work without replacing its post-send error."""
        execution_ready.set()
        try:
            await task
        except asyncio.CancelledError:
            raise
        except BaseException as execution_error:
            raise post_send_error from execution_error
        raise post_send_error

    def _ensure_known_task(self, message: BrokerMessage) -> None:
        """Reject direct sends for tasks that are not registered locally."""
        if self.find_task(message.task_name) is None:
            raise UnknownTaskError(task_name=message.task_name)

    async def _dispatch_message(self, message: BrokerMessage) -> None:
        """Execute a validated message inline or track it in the background."""
        if self.await_inplace:
            task = asyncio.create_task(
                self.receiver.callback(message=message.message),
            )
            self._track_inflight_task(task)
            await task
            return

        self._start_background_task(message)

    def _start_background_task(self, message: BrokerMessage) -> None:
        """Start and track one local receiver callback."""
        task = asyncio.create_task(self.receiver.callback(message=message.message))
        self._track_running_task(task)

    def _track_running_task(self, task: asyncio.Task[Any]) -> None:
        """Track one local receiver callback until it finishes."""
        self._running_tasks.add(task)
        task.add_done_callback(self._on_running_task_done)

    def _track_inflight_task(self, task: asyncio.Task[Any]) -> None:
        """Track execution owned by an active inline send."""
        self._inflight_tasks[task] = asyncio.current_task()
        task.add_done_callback(self._on_inflight_task_done)

    def _on_inflight_task_done(self, task: asyncio.Task[Any]) -> None:
        """Release execution after its sender has observed the result."""
        self._inflight_tasks.pop(task, None)

    def _promote_to_background(self, task: asyncio.Task[Any]) -> None:
        """Transfer execution ownership from the sender to broker drain."""
        self._inflight_tasks.pop(task, None)
        self._track_running_task(task)

    def _release_to_background(
        self,
        task: asyncio.Task[Any],
        execution_ready: asyncio.Event,
    ) -> None:
        """Transfer and release gated execution as one ownership transition."""
        self._promote_to_background(task)
        execution_ready.set()

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
            active_tasks = tuple(self._inflight_tasks.keys() | self._running_tasks)
            await asyncio.gather(
                *(asyncio.shield(task) for task in active_tasks),
                return_exceptions=True,
            )
            for task in active_tasks:
                self._inflight_tasks.pop(task, None)
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

        Cancellation is propagated after accepted work and resources are closed.
        """
        self._ensure_external_drain("shutdown")
        shutdown_error: BaseException | None = None

        while True:
            try:
                await self.wait_all()
            except asyncio.CancelledError as exc:
                shutdown_error = self._remember_shutdown_error(
                    shutdown_error,
                    exc,
                )
                continue
            except BaseException as exc:
                shutdown_error = self._remember_shutdown_error(
                    shutdown_error,
                    exc,
                )
            break

        try:
            await super().shutdown()
        except BaseException as exc:
            shutdown_error = self._remember_shutdown_error(shutdown_error, exc)

        try:
            self.executor.shutdown()
        except BaseException as exc:  # pragma: no cover
            shutdown_error = self._remember_shutdown_error(shutdown_error, exc)

        if shutdown_error is not None:
            raise shutdown_error
