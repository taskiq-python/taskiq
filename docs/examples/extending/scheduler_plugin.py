from taskiq import (
    InMemoryBroker,
    ScheduledTask,
    SchedulerPlugin,
    ScheduleSource,
    TaskiqScheduler,
)
from taskiq.schedule_sources import LabelScheduleSource

broker = InMemoryBroker()


class MyPlugin(SchedulerPlugin):
    def on_schedules_updated(
        self,
        source: ScheduleSource,
        schedules: list[ScheduledTask],
    ) -> None:
        print(f"Source {source} returned {len(schedules)} schedules.")

    async def post_send(self, source: ScheduleSource, task: ScheduledTask) -> None:
        print(f"Task {task.task_name} was sent with schedule_id {task.schedule_id}.")


scheduler = TaskiqScheduler(
    broker=broker,
    sources=[LabelScheduleSource(broker)],
    plugins=[MyPlugin()],
)
