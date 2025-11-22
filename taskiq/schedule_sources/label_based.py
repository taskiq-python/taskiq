import uuid
from logging import getLogger
from typing import Dict, List

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.scheduler.scheduled_task import ScheduledTask

logger = getLogger(__name__)


class LabelScheduleSource(ScheduleSource):
    """Schedule source based on labels."""

    def __init__(self, broker: AsyncBroker) -> None:
        self.broker = broker
        self.schedules: Dict[str, ScheduledTask] = {}

    async def startup(self) -> None:
        """
        Startup the schedule source.

        This function iterates over all tasks
        available to the broker and collects
        schedules from their labels.
        If task has a schedule label,
        it will be parsed and added to the
        scheduler list.

        Every time schedule is added, the random
        schedule id is generated. Please be aware that
        they are different for every startup.

        :return: None
        """
        self.schedules.clear()
        for task_name, task in self.broker.get_all_tasks().items():
            if task.broker != self.broker:
                # if task broker doesn't match self, something is probably wrong
                logger.warning(
                    f"Broker for {task_name} `{task.broker}` doesn't "
                    f"match scheduler's broker `{self.broker}`",
                )
                continue
            for schedule in task.labels.get("schedule", []):
                if not {"cron", "interval", "time"} & schedule.keys():
                    continue
                labels = schedule.get("labels", {})

                task_labels = {k: v for k, v in task.labels.items() if k != "schedule"}

                labels.update(task_labels)
                schedule_id = schedule.get("schedule_id", uuid.uuid4().hex)

                self.schedules[schedule_id] = ScheduledTask(
                    task_name=task_name,
                    labels=labels,
                    schedule_id=schedule_id,
                    args=schedule.get("args", []),
                    kwargs=schedule.get("kwargs", {}),
                    cron=schedule.get("cron"),
                    time=schedule.get("time"),
                    cron_offset=schedule.get("cron_offset"),
                    interval=schedule.get("interval"),
                )

        return await super().startup()

    async def get_schedules(self) -> List["ScheduledTask"]:
        """
        Collect schedules for all tasks.

        this function checks labels for all
        tasks available to the broker.

        If task has a schedule label,
        it will be parsed and returned.

        :return: list of schedules.
        """
        return list(self.schedules.values())

    def post_send(self, task: "ScheduledTask") -> None:
        """
        Remove `time` schedule from task's scheduler list.

        Task just have sent and won't be sent by that trigger anymore. Other triggers in
        scheduler list left unchanged.

        :param scheduled_task: task that just have sent
        """
        if task.cron or not task.time:
            return  # it's scheduled task with cron label, do not remove this trigger.

        self.schedules.pop(task.schedule_id, None)
