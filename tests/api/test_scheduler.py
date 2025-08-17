import asyncio
import contextlib
from datetime import datetime, timedelta, timezone

import pytest

from taskiq import TaskiqScheduler
from taskiq.api import run_scheduler_task
from taskiq.schedule_sources import LabelScheduleSource
from tests.utils import AsyncQueueBroker


@pytest.mark.anyio
async def test_successful() -> None:
    broker = AsyncQueueBroker()
    scheduler = TaskiqScheduler(broker, sources=[LabelScheduleSource(broker)])
    scheduler_task = asyncio.create_task(run_scheduler_task(scheduler))

    @broker.task(schedule=[{"time": datetime.now(timezone.utc) - timedelta(seconds=1)}])
    def _() -> None:
        ...

    msg = await asyncio.wait_for(broker.queue.get(), 1)
    assert msg

    scheduler_task.cancel()


@pytest.mark.anyio
async def test_cancelation() -> None:
    broker = AsyncQueueBroker()
    scheduler = TaskiqScheduler(broker, sources=[LabelScheduleSource(broker)])

    @broker.task(schedule=[{"time": datetime.now(timezone.utc)}])
    def _() -> None:
        ...

    scheduler_task = asyncio.create_task(run_scheduler_task(scheduler))

    msg = await asyncio.wait_for(broker.queue.get(), 1)
    assert msg

    scheduler_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await scheduler_task

    assert scheduler_task.cancelled()
