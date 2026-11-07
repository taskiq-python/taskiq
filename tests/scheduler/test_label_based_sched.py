import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from freezegun import freeze_time

from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.cli.scheduler.args import SchedulerArgs
from taskiq.cli.scheduler.run import run_scheduler
from taskiq.schedule_sources.label_based import LabelScheduleSource
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler


@pytest.mark.parametrize(
    "schedule_label",
    [
        pytest.param([{"cron": "* * * * *"}], id="cron"),
        pytest.param([{"time": datetime.now(timezone.utc)}], id="time"),
        pytest.param(
            [{"time": datetime.now(timezone.utc), "labels": {"foo": "bar"}}],
            id="labels_inside_schedule",
        ),
        pytest.param(
            [{"cron": "*/1 * * * *", "schedule_id": "every_minute"}],
            id="schedule_with_id",
        ),
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
    await source.startup()
    schedules = await source.get_schedules()
    assert schedules == [
        ScheduledTask(
            schedule_id=schedule_label[0].get("schedule_id", schedules[0].schedule_id),
            cron=schedule_label[0].get("cron"),
            time=schedule_label[0].get("time"),
            task_name="test_task",
            labels=schedule_label[0].get("labels", {}),
            args=[],
            kwargs={},
        ),
    ]

    # check that labels of tasks are not changed after startup and discovery process
    task_from_broker = next(iter(broker.get_all_tasks().values()))
    assert task_from_broker.labels == {"schedule": schedule_label}


async def test_label_discovery_no_cron() -> None:
    broker = InMemoryBroker()

    @broker.task(
        task_name="test_task",
        schedule=[{"args": ["* * * * *"]}],
    )
    def task() -> None:
        pass

    source = LabelScheduleSource(broker)
    await source.startup()
    schedules = await source.get_schedules()
    assert schedules == []


async def test_task_scheduled_at_time_runs_only_once(mock_sleep: None) -> None:
    event = asyncio.Event()
    broker = InMemoryBroker()
    scheduler = TaskiqScheduler(
        broker=broker,
        sources=[LabelScheduleSource(broker)],
    )
    for source in scheduler.sources:
        await source.startup()

    # NOTE:
    # freeze time to 00:00, so task won't be scheduled by `cron`, only by `time`
    with freeze_time("00:00:00", tick=True):

        @broker.task(
            task_name="test_task",
            schedule=[
                {"time": datetime.now(timezone.utc), "args": [1]},
                {"time": datetime.now(timezone.utc) + timedelta(days=1), "args": [2]},
                {"cron": "1 * * * *", "args": [3]},
            ],
        )
        def task(number: int) -> None:
            event.set()

        # Run scheduler
        loop = asyncio.get_running_loop()
        sched_task = loop.create_task(
            run_scheduler(SchedulerArgs(scheduler=scheduler, modules=[])),
        )

        # Wait for task be called
        await asyncio.wait_for(event.wait(), 2.0)

        # Wait again, but task is not called again as expected, so TimeoutError.
        event.clear()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(event.wait(), 2.0)

        # Check that other scheduled task are not effected and still available
        tasks = [task.args for task in await scheduler.sources[0].get_schedules()]
        assert tasks == [[2], [3]]  # [1] not in a list as it was enqueued above
    sched_task.cancel()


async def test_schedule_discovery_keeps_shared_labels_isolated() -> None:
    broker = InMemoryBroker()
    schedule = {"cron": "* * * * *", "labels": {"origin": "nightly"}}

    @broker.task(task_name="dedicated_task", schedule=[schedule], queue="dedicated")
    async def dedicated_task() -> None:
        pass

    @broker.task(task_name="default_task", schedule=[schedule])
    async def default_task() -> None:
        pass

    source = LabelScheduleSource(broker)
    for _ in range(2):
        await source.startup()
        schedules = {
            task.task_name: task.labels for task in await source.get_schedules()
        }
        assert schedules == {
            "dedicated_task": {"origin": "nightly", "queue": "dedicated"},
            "default_task": {"origin": "nightly"},
        }
        assert schedule["labels"] == {"origin": "nightly"}

    await broker.shutdown()


async def test_schedule_discovery_preserves_task_label_precedence() -> None:
    broker = InMemoryBroker()
    schedule = {
        "cron": "* * * * *",
        "labels": {"queue": "schedule", "origin": "nightly"},
    }

    @broker.task(task_name="task", schedule=[schedule], queue="task")
    async def task() -> None:
        pass

    source = LabelScheduleSource(broker)
    await source.startup()
    schedules = await source.get_schedules()

    assert schedules[0].labels == {"queue": "task", "origin": "nightly"}
    assert schedule["labels"] == {"queue": "schedule", "origin": "nightly"}
    await broker.shutdown()
