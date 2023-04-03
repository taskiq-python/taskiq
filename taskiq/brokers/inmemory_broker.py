from collections import OrderedDict
from typing import Any, AsyncGenerator, Callable, Optional, TypeVar

from aiochannel import Channel

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend, TaskiqResult
from taskiq.events import TaskiqEvents
from taskiq.exceptions import TaskiqError
from taskiq.message import BrokerMessage
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

    def __init__(
        self,
        max_stored_results: int = 100,
        result_backend: Optional[AsyncResultBackend[Any]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
    ) -> None:
        if result_backend is None:
            result_backend = InmemoryResultBackend(
                max_stored_results=max_stored_results,
            )
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self.channel: Channel[BrokerMessage] = Channel()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking task.

        This method just executes given task.

        :param message: incomming message.

        :raises TaskiqError: if someone wants to kick unknown task.
        """
        target_task = self.available_tasks.get(message.task_name)
        if target_task is None:
            raise TaskiqError("Unknown task.")
        await self.channel.put(message)

    async def listen(self) -> AsyncGenerator[BrokerMessage, None]:
        """
        Listen to channel.

        This function listens to channel and yields every new message.

        :yields: broker message.
        """
        async for message in self.channel:
            yield message

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
