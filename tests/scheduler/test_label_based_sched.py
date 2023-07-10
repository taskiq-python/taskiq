import asyncio
from datetime import datetime, timedelta
from typing import Any

import pytest
from freezegun import freeze_time

from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.cli.scheduler.args import SchedulerArgs
from taskiq.cli.scheduler.run import run_scheduler
from taskiq.schedule_sources.label_based import LabelScheduleSource
from taskiq.scheduler.scheduler import ScheduledTask, TaskiqScheduler


@pytest.mark.anyio
@pytest.mark.parametrize(
    "schedule_label",
    [
        pytest.param([{"cron": "* * * * *"}], id="cron"),
        pytest.param([{"time": datetime.utcnow()}], id="time"),
    ],
)
async def test_label_discovery(schedule_label: list[dict[str, Any]]) -> None:
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


@pytest.mark.anyio
async def test_task_scheduled_at_time_runs_only_once(mock_sleep: None) -> None:
    event = asyncio.Event()
    broker = InMemoryBroker()
    scheduler = TaskiqScheduler(
        broker=broker,
        sources=[LabelScheduleSource(broker)],
    )

    with freeze_time(tick=True):

        @broker.task(
            task_name="test_task",
            schedule=[
                {"time": datetime.utcnow(), "args": [1]},
                {"time": datetime.utcnow() + timedelta(days=1), "args": [2]},
                {"cron": "1 2 3 4 5", "args": [3]},
            ],
        )
        def task(number: int) -> None:
            event.set()

        # Run scheduler
        loop = asyncio.get_running_loop()
        loop.create_task(run_scheduler(SchedulerArgs(scheduler=scheduler, modules=[])))

        # Wait for task be called
        await asyncio.wait_for(event.wait(), 2.0)

        # Wait again, but task is not called again as expected, so TimeoutError.
        event.clear()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(event.wait(), 2.0)

        # Check that other scheduled task are not effected and is still available
        tasks = [task.args for task in await scheduler.sources[0].get_schedules()]
        assert tasks == [[2], [3]]  # [1] not in a list as it was enqueued above
