from typing import AsyncGenerator, Optional, TypeVar

from typing_extensions import ParamSpec

from taskiq.abc.broker import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.exceptions import TaskiqError
from taskiq.kicker import AsyncKicker
from taskiq.message import BrokerMessage

_ReturnType = TypeVar("_ReturnType")
_Params = ParamSpec("_Params")


class SharedDecoratedTask(AsyncTaskiqDecoratedTask[_Params, _ReturnType]):
    """Decorator that is used with shared broker."""

    def kicker(self) -> AsyncKicker[_Params, _ReturnType]:
        """
        This method updates getting default kicker.

        In this method we want to get default broker from
        our shared broker and send task to it, instead
        of shared_broker.

        :return: new kicker.
        """
        broker = getattr(self.broker, "_default_broker", None) or self.broker
        return AsyncKicker(
            task_name=self.task_name,
            broker=broker,
            labels=self.labels,
        )


class AsyncSharedBroker(AsyncBroker):
    """Broker for creating shared tasks."""

    def __init__(self) -> None:
        super().__init__(None)
        self._default_broker: Optional[AsyncBroker] = None
        self.decorator_class = SharedDecoratedTask

    def default_broker(self, new_broker: AsyncBroker) -> None:
        """
        Updates default broker.

        :param new_broker: new async broker to kick tasks with.
        """
        self._default_broker = new_broker

    async def kick(self, message: BrokerMessage) -> None:
        """
        Shared broker cannot kick tasks.

        :param message: message to send.
        :raises TaskiqError: if called.
        """
        raise TaskiqError(
            "You cannot use kiq directly on shared task "
            "without setting the default_broker.",
        )

    async def listen(self) -> AsyncGenerator[bytes, None]:  # type: ignore
        """
        Shared broker cannot listen to tasks.

        This method will throw an exception.

        :raises TaskiqError: if called.
        """
        raise TaskiqError("Shared broker cannot listen")


async_shared_broker = AsyncSharedBroker()
