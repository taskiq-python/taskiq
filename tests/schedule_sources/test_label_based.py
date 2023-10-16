from datetime import datetime
from typing import Any, Dict, List

import pytest

from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.schedule_sources.label_based import LabelScheduleSource
from taskiq.scheduler.scheduled_task import ScheduledTask


@pytest.mark.anyio
@pytest.mark.parametrize(
    "schedule_label",
    [
        pytest.param([{"cron": "* * * * *"}], id="cron"),
        pytest.param([{"time": datetime.utcnow()}], id="time"),
    ],
)
async def test_label_discovery(schedule_label: List[Dict[str, Any]]) -> None:
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
            cron=schedule_label[0].get("cron"),
            time=schedule_label[0].get("time"),
            task_name="test_task",
            labels={"schedule": schedule_label},
            args=[],
            kwargs={},
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
