from typing import TYPE_CHECKING, Awaitable, Callable

from taskiq.abc.broker import AsyncBroker
from taskiq.message import TaskiqMessage

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.state import TaskiqState


class Context:
    """Context class."""

    def __init__(
        self,
        message: TaskiqMessage,
        broker: AsyncBroker,
        task_idler: Callable[[float], Awaitable[None]],
    ) -> None:
        self.message = message
        self.broker = broker
        self.state: "TaskiqState" = None  # type: ignore
        self.state = broker.state
        self.task_idler = task_idler
