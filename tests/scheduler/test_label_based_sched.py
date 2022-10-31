import pytest

from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.schedule_sources.label_based import LabelScheduleSource
from taskiq.scheduler.scheduler import ScheduledTask


@pytest.mark.anyio
async def test_label_discovery() -> None:
    broker = InMemoryBroker()

    @broker.task(
        task_name="test_task",
        schedule=[{"cron": "* * * * *"}],
    )
    def task() -> None:
        pass

    source = LabelScheduleSource(broker)
    schedules = await source.get_schedules()
    assert schedules == [
        ScheduledTask(
            cron="* * * * *",
            task_name="test_task",
            labels={"schedule": [{"cron": "* * * * *"}]},
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
