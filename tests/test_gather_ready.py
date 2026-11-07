import asyncio
from typing import Any

from taskiq import AsyncTaskiqTask, TaskiqResult
from taskiq.brokers.inmemory_broker import InmemoryResultBackend
from taskiq.funcs import gather


async def test_ready_results_do_not_wait_for_polling_interval() -> None:
    backend: InmemoryResultBackend[Any] = InmemoryResultBackend()
    await backend.set_result(
        "ready",
        TaskiqResult(is_err=False, return_value=42, execution_time=0),
    )
    task: AsyncTaskiqTask[Any] = AsyncTaskiqTask(
        task_id="ready",
        result_backend=backend,
    )
    results = await asyncio.wait_for(gather(task, periodicity=5), timeout=0.5)
    assert results[0].return_value == 42


async def test_pending_results_are_still_polled() -> None:
    backend: InmemoryResultBackend[Any] = InmemoryResultBackend()
    task: AsyncTaskiqTask[Any] = AsyncTaskiqTask(
        task_id="later",
        result_backend=backend,
    )

    async def publish() -> None:
        await asyncio.sleep(0)
        await backend.set_result(
            "later",
            TaskiqResult(is_err=False, return_value=7, execution_time=0),
        )

    publisher = asyncio.create_task(publish())
    try:
        results = await asyncio.wait_for(gather(task, periodicity=0.01), timeout=1)
        assert results[0].return_value == 7
    finally:
        await publisher


async def test_empty_gather_still_returns_an_empty_tuple() -> None:
    assert await gather(periodicity=5) == ()


async def test_gather_preserves_order_and_duplicate_tasks() -> None:
    backend: InmemoryResultBackend[Any] = InmemoryResultBackend()
    tasks = []
    for task_id, value in [("first", 1), ("second", 2)]:
        await backend.set_result(
            task_id,
            TaskiqResult(is_err=False, return_value=value, execution_time=0),
        )
        tasks.append(
            AsyncTaskiqTask[Any](task_id=task_id, result_backend=backend),
        )
    results = await gather(tasks[1], tasks[0], tasks[1], periodicity=0)
    assert [result.return_value for result in results] == [2, 1, 2]
