from datetime import datetime, timezone
from typing import Any

from taskiq import Context, TaskiqDepends
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.exceptions import ScheduledTaskCancelledError
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler


class CancellingScheduleSource(ScheduleSource):
    async def get_schedules(self) -> list["ScheduledTask"]:
        """Return schedules list."""
        return []

    def pre_send(
        self,
        task: "ScheduledTask",
    ) -> None:
        """Raise cancelled error."""
        raise ScheduledTaskCancelledError


async def test_scheduled_task_cancelled() -> None:
    broker = InMemoryBroker()
    source = CancellingScheduleSource()
    scheduler = TaskiqScheduler(broker=broker, sources=[source])
    task = ScheduledTask(
        task_name="ping:pong",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )

    await scheduler.on_ready(source, task)  # error is caught


class _RecordingScheduleSource(ScheduleSource):
    def __init__(self) -> None:
        self.schedules: list[ScheduledTask] = []

    async def get_schedules(self) -> list[ScheduledTask]:
        return list(self.schedules)

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        self.schedules.append(schedule)


async def test_scheduled_task_preserves_label_types() -> None:
    broker = InMemoryBroker(await_inplace=True)
    source = _RecordingScheduleSource()
    scheduler = TaskiqScheduler(broker=broker, sources=[source])
    received: list[dict[str, Any]] = []

    @broker.task
    async def inspect_labels(context: Context = TaskiqDepends()) -> None:
        received.append(dict(context.message.labels))

    raw_labels: dict[str, Any] = {
        "count": 5,
        "ratio": 1.5,
        "flag": False,
        "data": b"hi",
    }
    kicker = inspect_labels.kicker().with_labels(**raw_labels)
    await kicker.schedule_by_cron(source, "* * * * *")
    await kicker.schedule_by_interval(source, 60)
    await kicker.schedule_by_time(source, datetime.now(timezone.utc))

    for stored in source.schedules:
        restored = ScheduledTask.model_validate(stored.model_dump(mode="json"))
        await scheduler.on_ready(source, restored)

    assert len(received) == 3
    for labels in received:
        assert labels["count"] == 5
        assert type(labels["count"]) is int
        assert labels["ratio"] == 1.5
        assert type(labels["ratio"]) is float
        assert labels["flag"] is False
        assert type(labels["flag"]) is bool
        assert labels["data"] == b"hi"
        assert type(labels["data"]) is bytes


async def test_scheduled_task_without_labels_types_still_works() -> None:
    broker = InMemoryBroker(await_inplace=True)
    source = _RecordingScheduleSource()
    scheduler = TaskiqScheduler(broker=broker, sources=[source])
    received: dict[str, Any] = {}

    @broker.task
    async def inspect_labels(context: Context = TaskiqDepends()) -> None:
        received.update(context.message.labels)

    task = ScheduledTask(
        task_name=inspect_labels.task_name,
        labels={"queue": "default", "retries": 2},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    await scheduler.on_ready(source, task)

    assert received["queue"] == "default"
    assert received["retries"] == 2
    assert type(received["retries"]) is int
    assert "schedule_id" not in task.labels
