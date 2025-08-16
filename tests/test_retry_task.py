import random

import pytest

from taskiq import (
    Context,
    InMemoryBroker,
    SmartRetryMiddleware,
    TaskiqDepends,
    TaskiqScheduler,
)
from taskiq.schedule_sources import LabelScheduleSource


@pytest.mark.parametrize(
    "retry_count",
    [0, random.randint(2, 5)],
)
@pytest.mark.anyio
async def test_save_task_id_for_retry(retry_count: int) -> None:
    broker = InMemoryBroker().with_middlewares(
        SmartRetryMiddleware(
            default_retry_count=retry_count + 1,
            default_delay=0.1,
        ),
    )
    scheduler = TaskiqScheduler(broker, [LabelScheduleSource(broker)])

    check_interval = 0.5

    @broker.task("exc_task", retry_on_error=True)
    async def exc_task(count: int = 0, context: "Context" = TaskiqDepends()) -> int:
        retry = int(context.message.labels.get("_retries", 0))
        if retry < count:
            raise Exception("test")
        return retry

    await broker.startup()
    await scheduler.startup()

    task_with_retry = await exc_task.kiq(retry_count)
    task_with_retry_result = await task_with_retry.wait_result(
        check_interval=check_interval,
    )
    assert task_with_retry_result.return_value == retry_count
