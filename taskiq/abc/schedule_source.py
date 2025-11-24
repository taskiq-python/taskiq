from abc import ABC, abstractmethod
from collections.abc import Coroutine
from types import CoroutineType
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.scheduler.scheduled_task import ScheduledTask


class ScheduleSource(ABC):
    """Abstract class for source of scheduled tasks."""

    async def startup(self) -> None:  # noqa: B027
        """Action to execute during startup."""

    async def shutdown(self) -> None:  # noqa: B027
        """Actions to execute during shutdown."""

    @abstractmethod
    async def get_schedules(self) -> list["ScheduledTask"]:
        """Get list of taskiq schedules."""

    async def add_schedule(
        self,
        schedule: "ScheduledTask",
    ) -> None:
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
        raise NotImplementedError(
            f"The source {self.__class__.__name__} does not support adding schedules.",
        )

    async def update_schedule(
        self,
        schedule: "ScheduledTask",
    ) -> None:
        """
        Update an existing schedule.

        This function is used to update existing schedules.
        It's a convenient helper for people who want to update schedules
        for the current source.

        As an example, if your source works with a database,
        you may want to update existing rows in the table.

        Note that this function may do nothing.

        :param schedule: schedule to update.
        """
        raise NotImplementedError(
            f"The source {self.__class__.__name__} does "
            "not support updating schedules.",
        )

    async def delete_schedule(self, schedule_id: str) -> None:
        """
        Method to delete schedule by id.

        This is useful for schedule cancelation.

        :param schedule_id: id of schedule to delete.
        """
        raise NotImplementedError(
            f"The source {self.__class__.__name__} does "
            "not support deleting schedules.",
        )

    def pre_send(  # noqa: B027
        self,
        task: "ScheduledTask",
    ) -> Union[None, "CoroutineType[Any, Any, None]", Coroutine[Any, Any, None]]:
        """
        Actions to execute before task will be sent to broker.

        This method may raise ScheduledTaskCancelledError.
        This cancels the task execution.

        :param task: task that will be sent
        """

    def post_send(  # noqa: B027
        self,
        task: "ScheduledTask",
    ) -> Union[None, "CoroutineType[Any, Any, None]", Coroutine[Any, Any, None]]:
        """
        Actions to execute after task was sent to broker.

        :param task: task that just have sent
        """
