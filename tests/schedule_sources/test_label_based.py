from datetime import datetime

import pytest

from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.schedule_sources.label_based import LabelScheduleSource
from taskiq.scheduler.scheduler import ScheduledTask


@pytest.mark.anyio
@pytest.mark.parametrize(
    "schedule_label",
    [
        pytest.param([{"cron": "* * * * *"}], id="cron"),
        pytest.param([{"time": datetime.utcnow()}], id="time"),
    ],
)
async def test_label_discovery(schedule_label: list[dict[str, str]]) -> None:
    broker = InMemoryBroker()

    @broker.task(
        task_name="test_task",
        schedule=schedule_label,
    )
    def task() -> None:
        pass

    source = LabelScheduleSource(broker)
    schedules = await source.get_schedules()
    assert schedules == [
        ScheduledTask(
            cron=schedule_label[0].get("cron"),  # type: ignore
            time=schedule_label[0].get("time"),  # type: ignore
            task_name="test_task",
            labels={"schedule": schedule_label},
            args=[],
            kwargs={},
            source=source,
        ),
    ]


@pytest.mark.anyio
async def test_label_discovery_no_cron() -> None:
    broker = InMemoryBroker()

    @broker.task(
        task_name="test_task",
        schedule=[{"args": ["* * * * *"]}],
    )
    def task() -> None:
        pass

    source = LabelScheduleSource(broker)
    schedules = await source.get_schedules()
    assert schedules == []
