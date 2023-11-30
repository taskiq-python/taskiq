import enum
from typing import Awaitable, Callable, Union

from pydantic import BaseModel


@enum.unique
class AcknowledgeType(enum.StrEnum):
    """Enum with possible acknowledge times."""

    # The message is acknowledged right when it's received,
    # before it's executed.
    WHEN_RECEIVED = enum.auto()
    # This option means that the message will be
    # acknowledged right after it's executed.
    WHEN_EXECUTED = enum.auto()
    # This option means that the message will be
    # acknowledged when the task will be saved
    # only after it's saved in the result backend.
    WHEN_SAVED = enum.auto()


class AckableMessage(BaseModel):
    """
    Message that can be acknowledged.

    If your broker support message acknowledgement,
    please return this type of message, so we'll be
    able to mark this message as acknowledged after
    the function will be executed.

    It adds more reliability to brokers and system
    as a whole.
    """

    data: bytes
    ack: Callable[[], Union[None, Awaitable[None]]]
