from taskiq.abc.broker import AsyncBroker
from taskiq.message import TaskiqMessage


class Context:
    """Context class."""

    def __init__(self, message: TaskiqMessage, broker: AsyncBroker) -> None:
        self.message = message
        self.broker = broker


default_context = Context(None, None)  # type: ignore
