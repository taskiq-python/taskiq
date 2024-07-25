from typing import TYPE_CHECKING, Any, Callable, Optional

from typing_extensions import TypeAlias

from taskiq.abc.broker import AsyncBroker
from taskiq.exceptions import NoResultError, TaskRejectedError
from taskiq.message import TaskiqMessage

if TYPE_CHECKING:  # pragma: no cover
    from contextlib import _AsyncGeneratorContextManager

    from taskiq.state import TaskiqState

    IdleType: TypeAlias = (
        "Callable[[Optional[int]], _AsyncGeneratorContextManager[None]]"
    )

else:
    IdleType: TypeAlias = Any


class Context:
    """Context class."""

    def __init__(
        self,
        message: TaskiqMessage,
        broker: AsyncBroker,
        idle: IdleType,
    ) -> None:
        self.message = message
        self.broker = broker
        self.state: "TaskiqState" = None  # type: ignore
        self.state = broker.state
        self.idle = idle

    async def requeue(self) -> None:
        """
        Requeue task.

        This fuction creates a task with
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
