from typing import TYPE_CHECKING

from taskiq.abc.broker import AsyncBroker
from taskiq.acks import AckController
from taskiq.compat import model_copy
from taskiq.exceptions import NoResultError, TaskRejectedError
from taskiq.message import TaskiqMessage

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.state import TaskiqState

_REQUEUE_LABEL = "X-Taskiq-requeue"


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
        Publish a replacement task and settle the current delivery.

        The current message is acknowledged only after the replacement has
        been published successfully. Non-ackable messages are still republished
        but cannot be settled by Taskiq.

        :raises NoResultError: to not store result for current task.
        """
        requeue_count = int(self.message.labels.get(_REQUEUE_LABEL, 0))
        requeue_count += 1
        requeue_value = str(requeue_count)
        requeued_message = model_copy(
            self.message,
            update={
                "labels": {
                    **self.message.labels,
                    _REQUEUE_LABEL: requeue_value,
                },
            },
        )

        if self._ack_controller is not None:
            self._ack_controller.start_requeue()
        try:
            await self.broker.router.requeue(requeued_message, broker=self.broker)
        except BaseException:
            if self._ack_controller is not None:
                self._ack_controller.fail_requeue()
            raise

        self.message.labels[_REQUEUE_LABEL] = requeue_value
        if self._ack_controller is not None:
            self._ack_controller.complete_requeue()
        raise NoResultError

    def reject(self) -> None:
        """
        Raise reject error.

        :raises TaskRejectedError: to reject current message.
        """
        raise TaskRejectedError
