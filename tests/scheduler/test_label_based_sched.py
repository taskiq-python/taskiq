import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from freezegun import freeze_time

from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.brokers.shared_broker import AsyncSharedBroker
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


async def test_label_discovery_shared_task(
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker = InMemoryBroker()
    shared_broker = AsyncSharedBroker()
    shared_broker.default_broker(broker)

    @shared_broker.task(task_name="shared_task", schedule=[{"cron": "* * * * *"}])
    def shared_task() -> None:
        pass

    @shared_broker.task(task_name="shared_task_no_schedule")
    def shared_task_no_schedule() -> None:
        pass

    source = LabelScheduleSource(broker)
    with caplog.at_level(logging.WARNING):
        await source.startup()

    schedules = await source.get_schedules()
    assert [schedule.task_name for schedule in schedules] == ["shared_task"]
    assert "doesn't match scheduler's broker" not in caplog.text


@pytest.mark.parametrize(
    "set_default_broker",
    [
        pytest.param(True, id="other_default_broker"),
        pytest.param(False, id="no_default_broker"),
    ],
)
async def test_label_discovery_shared_task_broker_mismatch(
    caplog: pytest.LogCaptureFixture,
    set_default_broker: bool,
) -> None:
    broker = InMemoryBroker()
    shared_broker = AsyncSharedBroker()
    if set_default_broker:
        shared_broker.default_broker(InMemoryBroker())

    @shared_broker.task(task_name="shared_task", schedule=[{"cron": "* * * * *"}])
    def shared_task() -> None:
        pass

    @shared_broker.task(task_name="shared_task_no_schedule")
    def shared_task_no_schedule() -> None:
        pass

    source = LabelScheduleSource(broker)
    with caplog.at_level(logging.WARNING):
        await source.startup()

    assert await source.get_schedules() == []
    # Only the task whose schedules are ignored is reported.
    warnings = [
        record.getMessage()
        for record in caplog.records
        if "doesn't match scheduler's broker" in record.getMessage()
    ]
    assert len(warnings) == 1
    assert warnings[0].startswith("Broker for shared_task `")


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
