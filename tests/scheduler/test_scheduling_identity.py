from datetime import datetime, timezone
from typing import Any, Literal

import pytest

from taskiq.abc.schedule_source import ScheduleSource
from taskiq.compat import model_dump
from taskiq.kicker import AsyncKicker
from taskiq.message import BrokerMessage
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler
from tests.utils import AsyncQueueBroker

ScheduleKind = Literal["cron", "interval", "time"]


class RecordingBroker(AsyncQueueBroker):
    """Record scheduler dispatch without using a transport."""

    def __init__(self) -> None:
        super().__init__()
        self.sent: list[BrokerMessage] = []

    async def kick(self, message: BrokerMessage) -> None:
        self.sent.append(message)


class RecordingScheduleSource(ScheduleSource):
    """Store schedules created through the public kicker API."""

    def __init__(self) -> None:
        self.schedules: list[ScheduledTask] = []

    async def get_schedules(self) -> list[ScheduledTask]:
        return self.schedules

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        self.schedules.append(schedule)


async def create_schedule(
    kind: ScheduleKind,
    kicker: AsyncKicker[Any, Any],
    source: RecordingScheduleSource,
) -> ScheduledTask:
    """Create one schedule through the selected public kicker method."""
    if kind == "cron":
        created = await kicker.schedule_by_cron(source, "* * * * *")
    elif kind == "interval":
        created = await kicker.schedule_by_interval(source, 10)
    else:
        created = await kicker.schedule_by_time(
            source,
            datetime(2030, 1, 1, tzinfo=timezone.utc),
        )
    return created.task


@pytest.mark.parametrize("kind", ["cron", "interval", "time"])
async def test_all_schedule_methods_preserve_custom_task_id(
    kind: ScheduleKind,
) -> None:
    broker = RecordingBroker()
    source = RecordingScheduleSource()
    kicker: AsyncKicker[Any, Any] = AsyncKicker("demo.task", broker, {})

    scheduled = await create_schedule(
        kind,
        kicker.with_task_id("custom-task-id"),
        source,
    )

    assert scheduled.task_id == "custom-task-id"
    assert source.schedules == [scheduled]


async def test_interval_without_custom_task_id_keeps_deferred_generation() -> None:
    broker = RecordingBroker()
    source = RecordingScheduleSource()
    kicker: AsyncKicker[Any, Any] = AsyncKicker("demo.task", broker, {})

    scheduled = await create_schedule("interval", kicker, source)

    assert scheduled.task_id is None


async def test_scheduler_reuses_stored_interval_task_id() -> None:
    broker = RecordingBroker()
    source = RecordingScheduleSource()
    kicker: AsyncKicker[Any, Any] = AsyncKicker("demo.task", broker, {})
    scheduled = await create_schedule(
        "interval",
        kicker.with_task_id("custom-task-id"),
        source,
    )
    restored = ScheduledTask(**model_dump(scheduled))

    scheduler = TaskiqScheduler(broker, [source])
    await scheduler.on_ready(source, restored)
    await scheduler.on_ready(source, restored)

    assert [message.task_id for message in broker.sent] == [
        "custom-task-id",
        "custom-task-id",
    ]


async def test_scheduler_generates_task_id_for_each_interval_dispatch() -> None:
    broker = RecordingBroker()
    source = RecordingScheduleSource()
    kicker: AsyncKicker[Any, Any] = AsyncKicker("demo.task", broker, {})
    scheduled = await create_schedule("interval", kicker, source)
    restored = ScheduledTask(**model_dump(scheduled))
    generated_task_ids = iter(("generated-task-id-1", "generated-task-id-2"))
    broker.with_id_generator(lambda: next(generated_task_ids))

    scheduler = TaskiqScheduler(broker, [source])
    await scheduler.on_ready(source, restored)
    await scheduler.on_ready(source, restored)

    assert [message.task_id for message in broker.sent] == [
        "generated-task-id-1",
        "generated-task-id-2",
    ]
