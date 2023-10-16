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
    # This method can be either sync or async.
    def add_schedule(self, schedule: "ScheduledTask") -> None:
        print("New schedule added:", schedule)

    # This method is optional. You may not implement this.
    # It's just a helper to people to be able to interact with your source.
    async def pre_send(self, task: "ScheduledTask") -> None:
        """
        Actions to execute before task will be sent to broker.

        :param task: task that will be sent
        """

    # This method is optional. You may not implement this.
    # It's just a helper to people to be able to interact with your source.
    async def post_send(self, task: "ScheduledTask") -> None:
        """
        Actions to execute after task was sent to broker.

        :param task: task that just have sent
        """
