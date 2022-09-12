from typing import List

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.scheduler.scheduler import ScheduledTask


class LabelScheduleSource(ScheduleSource):
    """Schedule source based on labels."""

    def __init__(self, broker: AsyncBroker) -> None:
        self.broker = broker

    async def get_schedules(self) -> List["ScheduledTask"]:
        """
        Collect schedules for all tasks.

        this function checks labels for all
        tasks available to the broker.

        If task has a schedule label,
        it will be parsed and retuned.

        :return: list of schedules.
        """
        schedules = []
        for task_name, task in self.broker.available_tasks.items():
            if task.broker != self.broker:
                continue
            for schedule in task.labels.get("schedule", []):
                labels = schedule.get("labels", {})
                labels.update(task.labels)
                schedules.append(
                    ScheduledTask(
                        task_name=task_name,
                        labels=labels,
                        args=schedule.get("args", []),
                        kwargs=schedule.get("kwargs", {}),
                        cron=schedule["cron"],
                    ),
                )
        return schedules
