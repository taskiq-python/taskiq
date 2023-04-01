import asyncio
import uuid

import pytest

from taskiq import InMemoryBroker
from taskiq.events import TaskiqEvents
from taskiq.state import TaskiqState


@pytest.mark.anyio
async def test_inmemory_success() -> None:
    broker = InMemoryBroker()
    test_val = uuid.uuid4().hex

    @broker.task
    async def task() -> str:
        return test_val

    kicked = await task.kiq()
    result = await kicked.wait_result()
    assert result.return_value == test_val
    assert not broker._running_tasks


@pytest.mark.anyio
async def test_cannot_listen() -> None:
    broker = InMemoryBroker()

    with pytest.raises(RuntimeError):
        async for _ in broker.listen():
            pass


@pytest.mark.anyio
async def test_startup() -> None:
    broker = InMemoryBroker()
    test_value = uuid.uuid4().hex

    @broker.on_event(TaskiqEvents.WORKER_STARTUP)
    async def _w_startup(state: TaskiqState) -> None:
        state.from_worker = test_value

    @broker.on_event(TaskiqEvents.CLIENT_STARTUP)
    async def _c_startup(state: TaskiqState) -> None:
        state.from_client = test_value

    await broker.startup()

    assert broker.state.from_worker == test_value
    assert broker.state.from_client == test_value


@pytest.mark.anyio
async def test_shutdown() -> None:
    broker = InMemoryBroker()
    test_value = uuid.uuid4().hex

    @broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
    async def _w_startup(state: TaskiqState) -> None:
        state.from_worker = test_value

    @broker.on_event(TaskiqEvents.CLIENT_SHUTDOWN)
    async def _c_startup(state: TaskiqState) -> None:
        state.from_client = test_value

    await broker.shutdown()

    assert broker.state.from_worker == test_value
    assert broker.state.from_client == test_value


@pytest.mark.anyio
async def test_execution() -> None:
    broker = InMemoryBroker()
    test_value = uuid.uuid4().hex

    @broker.task
    async def test_task() -> str:
        await asyncio.sleep(0.5)
        return test_value

    task = await test_task.kiq()
    assert not await task.is_ready()

    result = await task.wait_result()
    assert result.return_value == test_value
