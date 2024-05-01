from logging import getLogger
from typing import List

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.scheduler.scheduled_task import ScheduledTask

logger = getLogger(__name__)


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
        it will be parsed and returned.

        :return: list of schedules.
        """
        schedules = []
        for task_name, task in self.broker.get_all_tasks().items():
            if task.broker != self.broker:
                continue
            for schedule in task.labels.get("schedule", []):
                if all(field not in schedule for field in ("cron", "time", "period")):
                    continue
                labels = schedule.get("labels", {})
                labels.update(task.labels)
                schedules.append(
                    ScheduledTask(
                        task_name=task_name,
                        labels=labels,
                        args=schedule.get("args", []),
                        kwargs=schedule.get("kwargs", {}),
                        cron=schedule.get("cron"),
                        time=schedule.get("time"),
                        period=schedule.get("period"),
                        cron_offset=schedule.get("cron_offset"),
                    ),
                )
        return schedules

    def post_send(self, scheduled_task: ScheduledTask) -> None:
        """
        Remove `time` schedule from task's scheduler list.

        Task just have sent and won't be sent by that trigger anymore. Other triggers in
        scheduler list left unchanged.

        :param scheduled_task: task that just have sent
        """
        if scheduled_task.cron or not scheduled_task.time:
            return  # it's scheduled task with cron label, do not remove this trigger.

        for task_name, task in self.broker.get_all_tasks().items():
            if task.broker != self.broker or scheduled_task.task_name != task_name:
                continue

            schedule_list = task.labels.get("schedule", []).copy()
            for idx, schedule in enumerate(schedule_list):
                if schedule.get("time") == scheduled_task.time:
                    task.labels.get("schedule", []).pop(idx)
                    return
