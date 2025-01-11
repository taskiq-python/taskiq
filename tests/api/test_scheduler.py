import asyncio
import contextlib
from datetime import datetime, timedelta

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
    await asyncio.sleep(1)  # waiting start

    @broker.task(schedule=[{"time": datetime.utcnow() - timedelta(seconds=1)}])
    def _() -> None:
        ...

    msg = await asyncio.wait_for(broker.queue.get(), 0.3)
    assert msg

    scheduler_task.cancel()


@pytest.mark.anyio
async def test_cancelation() -> None:
    broker = AsyncQueueBroker()
    scheduler = TaskiqScheduler(broker, sources=[LabelScheduleSource(broker)])

    @broker.task(schedule=[{"time": datetime.utcnow()}])
    def _() -> None:
        ...

    scheduler_task = asyncio.create_task(run_scheduler_task(scheduler))
    await asyncio.sleep(1)  # waiting start

    msg = await asyncio.wait_for(broker.queue.get(), 0.3)
    assert msg

    scheduler_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await scheduler_task

    assert scheduler_task.cancelled()
