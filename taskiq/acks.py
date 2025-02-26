import enum
from typing import Awaitable, Callable, Union

from taskiq.message import DeliveryCountMessage, WrappedMessage


@enum.unique
class AcknowledgeType(str, enum.Enum):
    """Enum with possible acknowledge times."""

    # The message is acknowledged right when it's received,
    # before it's executed.
    WHEN_RECEIVED = "when_received"
    # This option means that the message will be
    # acknowledged right after it's executed.
    WHEN_EXECUTED = "when_executed"
    # This option means that the message will be
    # acknowledged when the task will be saved
    # only after it's saved in the result backend.
    WHEN_SAVED = "when_saved"


class AckableMessage(WrappedMessage):
    """
    Message that can be acknowledged.

    If your broker support message acknowledgement,
    please return this type of message, so we'll be
    able to mark this message as acknowledged after
    the function will be executed.

    It adds more reliability to brokers and system
    as a whole.
    """

    ack: Callable[[], Union[None, Awaitable[None]]]


class AckableMessageWithDeliveryCount(AckableMessage, DeliveryCountMessage):
    """Message that can be acknowledged and has a delivery count."""
