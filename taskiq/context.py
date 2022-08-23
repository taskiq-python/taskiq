from contextlib import contextmanager
from typing import Generator

from taskiq.abc.broker import AsyncBroker
from taskiq.message import TaskiqMessage


class Context:
    """Context class."""

    def __init__(self, message: TaskiqMessage, broker: AsyncBroker) -> None:
        self.message = message
        self.broker = broker


default_context = Context(None, None)  # type: ignore
current_context = None


@contextmanager
def context_updater(new_context: Context) -> Generator[None, None, None]:
    """
    Update context for some time.

    :param new_context: new context to set.
    :yield: nothing.
    """
    global current_context  # noqa: WPS420
    current_context = new_context  # noqa: WPS442

    yield

    current_context = None  # noqa: WPS442


def get_context() -> Context:
    """
    Get current context.

    This function always return contexts,
    but if you call this function inside tests,
    or somewhere you have to be careful,
    since if current_context is None it will
    return default_context.

    To override context please use context_updater
    context manager.

    :return: context.
    """
    global current_context  # noqa: WPS420
    if current_context is None:
        return default_context
    return current_context
