from typing import TYPE_CHECKING

from taskiq.abc.broker import AsyncBroker
from taskiq.exceptions import NoResultError, TaskRejectedError
from taskiq.message import TaskiqMessage

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.state import TaskiqState


class Context:
    """Context class."""

    def __init__(self, message: TaskiqMessage, broker: AsyncBroker) -> None:
        self.message = message
        self.broker = broker
        self.state: "TaskiqState" = None  # type: ignore
        self.state = broker.state

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
