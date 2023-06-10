import pytest

from taskiq import Context, InMemoryBroker, TaskiqDepends


@pytest.mark.anyio
async def test_requeue() -> None:
    broker = InMemoryBroker()

    runs_count = 0

    @broker.task
    async def task(context: Context = TaskiqDepends()) -> None:
        nonlocal runs_count
        runs_count += 1
        if runs_count < 2:
            await context.requeue()

    kicked = await task.kiq()
    await kicked.wait_result()

    assert runs_count == 2
