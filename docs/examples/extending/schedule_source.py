from typing import List

from taskiq import ScheduledTask, ScheduleSource


class MyScheduleSource(ScheduleSource):
    async def startup(self) -> None:
        """Do something when starting broker."""

    async def shutdown(self) -> None:
        """Do something on shutdown."""

    async def get_schedules(self) -> List["ScheduledTask"]:
        # Here you must return list of scheduled tasks from your source.
        return [
            ScheduledTask(
                task_name="",
                labels={},
                args=[],
                kwargs={},
                cron="* * * * *",
            ),
        ]

    # This method is optional. You may not implement this.
    # It's just a helper to people to be able to interact with your source.
    async def add_schedule(self, schedule: "ScheduledTask") -> None:
        return await super().add_schedule(schedule)
