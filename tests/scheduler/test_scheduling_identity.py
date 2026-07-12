from datetime import datetime, timezone
from typing import Any, Literal

import pytest

from taskiq.abc.schedule_source import ScheduleSource
from taskiq.kicker import AsyncKicker
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler
from tests.utils import RecordingBroker

ScheduleKind = Literal["cron", "interval", "time"]


class RecordingScheduleSource(ScheduleSource):
    """Store schedules created by a kicker."""

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


@pytest.mark.parametrize(
    ("custom_task_id", "expected_task_id"),
    [
        pytest.param("custom-task-id", "custom-task-id", id="custom"),
        pytest.param(None, "generated-task-id", id="generated-at-dispatch"),
    ],
)
async def test_scheduler_dispatch_uses_stored_or_generated_interval_task_id(
    custom_task_id: str | None,
    expected_task_id: str,
) -> None:
    broker = RecordingBroker()
    broker.with_id_generator(lambda: "generated-task-id")
    source = RecordingScheduleSource()
    kicker: AsyncKicker[Any, Any] = AsyncKicker("demo.task", broker, {})
    kicker.with_task_id(custom_task_id)
    scheduled = await create_schedule("interval", kicker, source)
    scheduler = TaskiqScheduler(broker, [source])

    await scheduler.on_ready(source, scheduled)

    assert broker.sent[0][0].task_id == expected_task_id
