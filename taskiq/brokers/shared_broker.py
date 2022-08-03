from typing import AsyncGenerator, Optional, TypeVar

from taskiq.abc.broker import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.exceptions import TaskiqError
from taskiq.kicker import AsyncKicker
from taskiq.message import TaskiqMessage
from taskiq.types_helpers import ReturnType_

Params_ = TypeVar("Params_")  # noqa: WPS120


class SharedDecoratedTask(AsyncTaskiqDecoratedTask[Params_, ReturnType_]):
    """Decorator that is used with shared broker."""

    def kicker(self) -> AsyncKicker[Params_, ReturnType_]:
        """
        This method updates getting default kicker.

        In this method we want to get default broker from
        our shared broker and send task to it, instead
        of shared_broker.

        :raises TaskiqError: if _default_broker is not set.
        :return: new kicker.
        """
        broker = getattr(self.broker, "_default_broker", None)
        if broker is None:
            raise TaskiqError(
                "You cannot use kiq directly on shared task "
                "without setting the default_broker.",
            )
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

    async def kick(self, message: TaskiqMessage) -> None:
        """
        Shared broker cannot kick tasks.

        :param message: message to send.
        :raises TaskiqError: if called.
        """
        raise TaskiqError("Shared broker cannot kick tasks.")

    def default_broker(self, new_broker: AsyncBroker) -> None:
        """
        Updates default broker.

        :param new_broker: new async broker to kick tasks with.
        """
        self._default_broker = new_broker

    async def listen(self) -> AsyncGenerator[TaskiqMessage, None]:  # type: ignore
        """
        Shared broker cannot listen to tasks.

        This method will throw an exception.

        :raises TaskiqError: if called.
        """
        raise TaskiqError("Shared broker cannot listen")


async_shared_broker = AsyncSharedBroker()
