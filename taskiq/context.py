from typing import TYPE_CHECKING

from taskiq.abc.broker import AsyncBroker
from taskiq.message import TaskiqMessage

if TYPE_CHECKING:
    from taskiq.state import TaskiqState


class Context:
    """Context class."""

    def __init__(self, message: TaskiqMessage, broker: AsyncBroker) -> None:
        self.message = message
        self.broker = broker
        self.state: "TaskiqState" = None  # type: ignore
        if broker:
            self.state = broker.state


default_context = Context(None, None)  # type: ignore
