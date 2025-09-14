import asyncio
import time
from typing import List

import pytest

from taskiq import TaskiqScheduler
from taskiq.api import run_scheduler_task
from taskiq.schedule_sources import LabelScheduleSource
from tests.utils import AsyncQueueBroker


@pytest.mark.anyio
async def test_interval_task_performance() -> None:
    broker = AsyncQueueBroker()
    scheduler = TaskiqScheduler(broker, sources=[LabelScheduleSource(broker)])

    @broker.task(schedule=[{"interval": 1, "args": [1]}])  # Every 1 second
    def performance_task(value: int) -> int:
        return value + 1

    scheduler_task = asyncio.create_task(run_scheduler_task(scheduler))

    try:
        execution_times: List[float] = []

        # Wait for 5 executions
        for _ in range(5):
            msg = await asyncio.wait_for(broker.queue.get(), 2)
            execution_times.append(time.time())
            assert msg is not None

        # Check intervals between executions
        for i in range(1, len(execution_times)):
            interval = execution_times[i] - execution_times[i - 1]
            assert 0.95 <= interval <= 1.05  # Allow some tolerance

    finally:
        scheduler_task.cancel()
