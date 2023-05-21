import asyncio
import inspect
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, AsyncGenerator, Set, TypeVar, get_type_hints

from taskiq_dependencies import DependencyGraph

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend, TaskiqResult
from taskiq.events import TaskiqEvents
from taskiq.exceptions import TaskiqError
from taskiq.message import BrokerMessage
from taskiq.receiver import Receiver
from taskiq.utils import maybe_awaitable

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

    async def set_result(self, task_id: str, result: TaskiqResult[_ReturnType]) -> None:
        """
        Sets result.

        This method is used to store result of an execution in a
        results dict. But also it removes previous results
        to keep memory footprint as low as possible.

        :param task_id: id of a task.
        :param result: result of an execution.
        """
        if self.max_stored_results != -1:
            if len(self.results) >= self.max_stored_results:
                self.results.popitem(last=False)
        self.results[task_id] = result

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Checks wether result is ready.

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


class InMemoryBroker(AsyncBroker):
    """
    This broker is used to execute tasks without sending them elsewhere.

    It's useful for local development, if you don't want to setup real broker.
    """

    def __init__(  # noqa: WPS211
        self,
        sync_tasks_pool_size: int = 4,
        max_stored_results: int = 100,
        cast_types: bool = True,
        max_async_tasks: int = 30,
        propagate_exceptions: bool = True,
    ) -> None:
        super().__init__()
        self.result_backend = InmemoryResultBackend(
            max_stored_results=max_stored_results,
        )
        self.executor = ThreadPoolExecutor(sync_tasks_pool_size)
        self.receiver = Receiver(
            broker=self,
            executor=self.executor,
            validate_params=cast_types,
            max_async_tasks=max_async_tasks,
            propagate_exceptions=propagate_exceptions,
        )
        self._running_tasks: "Set[asyncio.Task[Any]]" = set()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking task.

        This method just executes given task.

        :param message: incoming message.

        :raises TaskiqError: if someone wants to kick unknown task.
        """
        target_task = self.available_tasks.get(message.task_name)
        if target_task is None:
            raise TaskiqError("Unknown task.")

        if not self.receiver.dependency_graphs.get(target_task.task_name):
            self.receiver.dependency_graphs[target_task.task_name] = DependencyGraph(
                target_task.original_func,
            )
        if not self.receiver.task_signatures.get(target_task.task_name):
            self.receiver.task_signatures[target_task.task_name] = inspect.signature(
                target_task.original_func,
            )
        if not self.receiver.task_hints.get(target_task.task_name):
            self.receiver.task_hints[target_task.task_name] = get_type_hints(
                target_task.original_func,
            )

        task = asyncio.create_task(self.receiver.callback(message=message.message))
        self._running_tasks.add(task)
        task.add_done_callback(self._running_tasks.discard)

    def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Inmemory broker cannot listen.

        This method throws RuntimeError if you call it.
        Because inmemory broker cannot really listen to any of tasks.

        :raises RuntimeError: if this method is called.
        """
        raise RuntimeError("Inmemory brokers cannot listen.")

    async def startup(self) -> None:
        """Runs startup events for client and worker side."""
        for event in (TaskiqEvents.CLIENT_STARTUP, TaskiqEvents.WORKER_STARTUP):
            for handler in self.event_handlers.get(event, []):
                await maybe_awaitable(handler(self.state))

    async def shutdown(self) -> None:
        """Runs shutdown events for client and worker side."""
        for event in (TaskiqEvents.CLIENT_SHUTDOWN, TaskiqEvents.WORKER_SHUTDOWN):
            for handler in self.event_handlers.get(event, []):
                await maybe_awaitable(handler(self.state))
        self.executor.shutdown()
