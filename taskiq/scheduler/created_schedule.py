from typing import TYPE_CHECKING, Any, Coroutine, Generic, TypeVar, overload

from taskiq.abc.schedule_source import ScheduleSource
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.task import AsyncTaskiqTask

if TYPE_CHECKING:
    from taskiq.kicker import AsyncKicker

_ReturnType = TypeVar("_ReturnType")
_T = TypeVar("_T")


class CreatedSchedule(Generic[_ReturnType]):
    """A schedule that has been created."""

    def __init__(
        self,
        kicker: "AsyncKicker[Any,_ReturnType]",
        source: ScheduleSource,
        task: ScheduledTask,
    ) -> None:
        self.kicker = kicker
        self.source = source
        self.task = task
        self.schedule_id = task.schedule_id

    @overload
    async def kiq(
        self: "CreatedSchedule[Coroutine[Any,Any, _T]]",
    ) -> AsyncTaskiqTask[_T]:
        ...

    @overload
    async def kiq(self: "CreatedSchedule[_ReturnType]") -> AsyncTaskiqTask[_ReturnType]:
        ...

    async def kiq(self) -> Any:
        """Kick the task as if you were not scheduling it."""
        return await self.kicker.kiq(
            *self.task.args,
            **self.task.kwargs,
        )

    async def unschedule(self) -> None:
        """Unschedule the task."""
        await self.source.delete_schedule(self.task.schedule_id)

    def __str__(self) -> str:
        return (
            "CreatedSchedule("
            f"id={self.schedule_id}, "
            f"time={self.task.time}, "
            f"cron={self.task.cron}, "
            f"cron_offset={self.task.cron_offset or 'UTC'}, "
            f"task_name={self.task.task_name}, "
            f"args={self.task.args}, "
            f"kwargs={self.task.kwargs})"
        )
