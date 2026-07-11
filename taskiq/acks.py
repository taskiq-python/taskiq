import enum
from collections.abc import Awaitable, Callable
from typing import Any

from pydantic import BaseModel

from taskiq.utils import maybe_awaitable


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
    # This option means that the task is responsible
    # for acknowledging the message through Context.ack.
    MANUAL = "manual"


def parse_acknowledge_type(value: Any) -> AcknowledgeType:
    """Parse acknowledge type from a task label value."""
    if isinstance(value, AcknowledgeType):
        return value
    if isinstance(value, str):
        try:
            return AcknowledgeType(value.lower())
        except ValueError as exc:
            raise ValueError(f"Unknown acknowledge type value: {value}.") from exc
    raise ValueError(
        f"Unsupported acknowledge type: {value} use str or AcknowledgeType",
    )


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
    ack: Callable[[], None | Awaitable[None]]


class _RequeueState(enum.Enum):
    NOT_REQUESTED = enum.auto()
    PUBLISHING = enum.auto()
    PUBLISHED = enum.auto()
    FAILED = enum.auto()


class AckController:
    """Controls acknowledgement state for a received message."""

    def __init__(self, ack: Callable[[], None | Awaitable[None]] | None) -> None:
        self._ack = ack
        self._requeue_state = _RequeueState.NOT_REQUESTED
        self.is_acked = False

    @property
    def is_ackable(self) -> bool:
        """Whether the current message supports acknowledgement."""
        return self._ack is not None

    @property
    def requeue_published(self) -> bool:
        """Whether a replacement message was published successfully."""
        return self._requeue_state == _RequeueState.PUBLISHED

    @property
    def requeue_failed(self) -> bool:
        """Whether replacement publication failed or was cancelled."""
        return self._requeue_state == _RequeueState.FAILED

    def start_requeue(self) -> None:
        """Mark replacement publication as in progress."""
        self._requeue_state = _RequeueState.PUBLISHING

    def complete_requeue(self) -> None:
        """Mark replacement publication as successful."""
        self._requeue_state = _RequeueState.PUBLISHED

    def fail_requeue(self) -> None:
        """Mark replacement publication as failed or cancelled."""
        self._requeue_state = _RequeueState.FAILED

    async def ack(self) -> None:
        """Acknowledge the current message once."""
        if self._ack is None:
            raise RuntimeError("Current message is not ackable.")
        if self.is_acked:
            return
        await maybe_awaitable(self._ack())
        self.is_acked = True
