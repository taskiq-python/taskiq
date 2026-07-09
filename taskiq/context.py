from typing import TYPE_CHECKING

from taskiq.abc.broker import AsyncBroker
from taskiq.acks import AckController
from taskiq.exceptions import NoResultError, TaskRejectedError
from taskiq.message import TaskiqMessage

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.state import TaskiqState


class Context:
    """Context class."""

    def __init__(
        self,
        message: TaskiqMessage,
        broker: AsyncBroker,
        ack_controller: AckController | None = None,
    ) -> None:
        self.message = message
        self.broker = broker
        self._ack_controller = ack_controller
        self.state: TaskiqState = None  # type: ignore
        self.state = broker.state

    @property
    def is_ackable(self) -> bool:
        """Whether the current message supports acknowledgement."""
        return self._ack_controller is not None and self._ack_controller.is_ackable

    @property
    def is_acked(self) -> bool:
        """Whether the current message has already been acknowledged."""
        return self._ack_controller is not None and self._ack_controller.is_acked

    async def ack(self) -> None:
        """
        Acknowledge current message.

        :raises RuntimeError: if current broker message is not ackable.
        """
        if self._ack_controller is None:
            raise RuntimeError("Current message is not ackable.")
        await self._ack_controller.ack()

    async def requeue(self) -> None:
        """
        Requeue task.

        This function creates a task with
        the same message and sends it using
        current broker.

        :raises NoResultError: to not store result for current task.
        """
        requeue_count = int(self.message.labels.get("X-Taskiq-requeue", 0))
        requeue_count += 1
        self.message.labels["X-Taskiq-requeue"] = str(requeue_count)
        await self.broker.kick(self.broker.formatter.dumps(self.message))
        raise NoResultError

    def reject(self) -> None:
        """
        Raise reject error.

        :raises TaskRejectedError: to reject current message.
        """
        raise TaskRejectedError
