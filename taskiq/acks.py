import enum
from typing import Awaitable, Callable, Union

from pydantic import BaseModel


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


@enum.unique
class AckType(enum.Enum):
    """List of taskiq broker ack types."""

    # Acknowledges the message as soon as it is received.
    ON_RECEIVE = "ON_RECEIVE"
    # Acknowledges the message after the task is completed.
    ON_COMPLETE = "ON_COMPLETE"
    # Acknowledges the message after the task is completed and the result is
    # sent back to the client.
    ON_RESULT = "ON_RESULT"