import asyncio
from collections import OrderedDict
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend, TaskiqResult
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.events import TaskiqEvents
from taskiq.exceptions import UnknownTaskError
from taskiq.flow import FlowProtocol
from taskiq.message import BrokerMessage
from taskiq.receiver import Receiver
from taskiq.router import TaskiqRouter

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
        *,
        router: TaskiqRouter | None = None,
        broker_name: str | None = None,
        default_flow: FlowProtocol | None = None,
    ) -> None:
        super().__init__(
            router=router,
            broker_name=broker_name,
            default_flow=default_flow,
        )
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
        self._running_tasks: set[asyncio.Task[Any]] = set()
        self._running_task_error: BaseException | None = None

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking task.

        This method just executes given task.

        :param message: incoming message.

        :raises TaskiqError: if someone wants to kick unknown task.
        """
        target_task = self.find_task(message.task_name)
        if target_task is None:
            raise UnknownTaskError(task_name=message.task_name)

        receiver_cb = self.receiver.callback(message=message.message)
        if self.await_inplace:
            await receiver_cb
            return

        task = asyncio.create_task(receiver_cb)
        self._running_tasks.add(task)
        task.add_done_callback(self._on_running_task_done)

    async def kick_to_flow(
        self,
        message: BrokerMessage,
        flow: FlowProtocol | None = None,
    ) -> None:
        """Execute locally because every flow shares one in-memory runtime."""
        await self.kick(message)

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
        """
        while self._running_tasks:
            await asyncio.gather(
                *tuple(self._running_tasks),
                return_exceptions=True,
            )

        if self._running_task_error is not None:
            task_error = self._running_task_error
            self._running_task_error = None
            raise task_error

    def _on_running_task_done(self, task: asyncio.Task[Any]) -> None:
        """Release a completed task and retain failures until lifecycle drain."""
        self._running_tasks.discard(task)
        try:
            task.result()
        except BaseException as exc:
            if self._running_task_error is None:
                self._running_task_error = exc

    def _get_startup_events(self) -> tuple[TaskiqEvents, ...]:
        """Run both sides because this broker executes tasks in the client process."""
        return (TaskiqEvents.CLIENT_STARTUP, TaskiqEvents.WORKER_STARTUP)

    def _get_shutdown_events(self) -> tuple[TaskiqEvents, ...]:
        """Shut down both client and worker event phases in their legacy order."""
        return (TaskiqEvents.CLIENT_SHUTDOWN, TaskiqEvents.WORKER_SHUTDOWN)

    async def shutdown(self) -> None:
        """Drain local execution, close broker resources and stop the executor."""
        shutdown_error: BaseException | None = None

        try:
            await self.wait_all()
        except BaseException as exc:
            shutdown_error = exc

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
