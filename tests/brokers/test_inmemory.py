import asyncio
import uuid

import pytest

from taskiq import InMemoryBroker
from taskiq.events import TaskiqEvents
from taskiq.state import TaskiqState


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


async def test_cannot_listen() -> None:
    broker = InMemoryBroker()

    with pytest.raises(RuntimeError):
        async for _ in broker.listen():
            pass


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


async def test_inline_awaits() -> None:
    broker = InMemoryBroker(await_inplace=True)
    slept = False

    @broker.task
    async def test_task() -> None:
        nonlocal slept
        await asyncio.sleep(0.2)
        slept = True

    task = await test_task.kiq()
    assert slept
    assert await task.is_ready()
    assert not broker._running_tasks


async def test_wait_all() -> None:
    broker = InMemoryBroker()
    slept = False

    @broker.task
    async def test_task() -> None:
        nonlocal slept
        await asyncio.sleep(0.2)
        slept = True

    task = await test_task.kiq()
    assert not slept
    await broker.wait_all()
    assert slept
    assert await task.is_ready()
    assert not broker._running_tasks


async def test_batch_flush_on_size() -> None:
    broker = InMemoryBroker()
    seen: list[list[int]] = []

    @broker.task(batch=True, batch_size=3)
    async def batched(items: list[int]) -> int:
        seen.append(items)
        return sum(items)

    tasks = [await batched.kiq(i) for i in range(3)]
    results = [await t.wait_result(timeout=2) for t in tasks]

    assert seen == [[0, 1, 2]]
    assert [r.return_value for r in results] == [3, 3, 3]


async def test_batch_flush_on_wait_all() -> None:
    broker = InMemoryBroker()
    seen: list[list[int]] = []

    @broker.task(batch=True, batch_size=100, batch_timeout=30)
    async def batched(items: list[int]) -> int:
        seen.append(items)
        return sum(items)

    # Fewer than batch_size, so nothing runs until we flush.
    tasks = [await batched.kiq(i) for i in (10, 20)]
    assert seen == []

    await broker.wait_all()

    assert seen == [[10, 20]]
    results = [await t.wait_result(timeout=2) for t in tasks]
    assert [r.return_value for r in results] == [30, 30]


async def test_batch_await_inplace_flushes_immediately() -> None:
    broker = InMemoryBroker(await_inplace=True)
    seen: list[list[int]] = []

    @broker.task(batch=True, batch_size=100, batch_timeout=30)
    async def batched(items: list[int]) -> int:
        seen.append(items)
        return sum(items)

    task = await batched.kiq(7)
    # With await_inplace each kiq flushes a one-item batch right away.
    assert seen == [[7]]
    assert await task.is_ready()
    result = await task.wait_result(timeout=2)
    assert result.return_value == 7


async def test_batch_error_marks_all() -> None:
    broker = InMemoryBroker()

    @broker.task(batch=True, batch_size=2)
    async def batched(items: list[int]) -> int:
        raise ValueError("boom")

    tasks = [await batched.kiq(i) for i in (1, 2)]
    results = [await t.wait_result(timeout=2) for t in tasks]

    assert all(r.is_err for r in results)

