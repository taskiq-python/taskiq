import asyncio
import time

from taskiq import TaskiqScheduler
from taskiq.api import run_scheduler_task
from taskiq.schedule_sources import LabelScheduleSource
from tests.utils import AsyncQueueBroker


async def test_interval_task_performance() -> None:
    broker = AsyncQueueBroker()
    scheduler = TaskiqScheduler(broker, sources=[LabelScheduleSource(broker)])

    @broker.task(schedule=[{"interval": 1, "args": [1]}])  # Every 1 second
    def performance_task(value: int) -> int:
        return value + 1

    scheduler_task = asyncio.create_task(run_scheduler_task(scheduler))

    try:
        execution_times: list[float] = []

        # Wait for 5 executions
        for _ in range(5):
            msg = await asyncio.wait_for(broker.queue.get(), 3)
            execution_times.append(time.time())
            assert msg is not None

        intervals = [
            execution_times[i] - execution_times[i - 1]
            for i in range(1, len(execution_times))
        ]
        for interval in intervals:
            assert 0.5 <= interval <= 3  # Loose bound to catch gross regressions

        average_interval = sum(intervals) / len(intervals)
        assert 0.8 <= average_interval <= 1.5

    finally:
        scheduler_task.cancel()
