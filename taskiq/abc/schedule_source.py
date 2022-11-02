from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.scheduler.scheduler import ScheduledTask


class ScheduleSource(ABC):
    """Abstract class for source of scheduled tasks."""

    async def startup(self) -> None:  # noqa: B027
        """Action to execute during startup."""

    async def shutdown(self) -> None:  # noqa: B027
        """Actions to execute during shutdown."""

    @abstractmethod
    async def get_schedules(self) -> List["ScheduledTask"]:
        """Get list of taskiq schedules."""

    async def add_schedule(self, schedule: "ScheduledTask") -> None:  # noqa: B027
        """
        Add a new schedule.

        This function is used to add new schedules.
        It's a convenient helper for people who want to add new schedules
        for the current source.

        As an example, if your source works with a database,
        you may want to add new rows to the table.

        Note that this function may do nothing.

        :param schedule: schedule to add.
        """
