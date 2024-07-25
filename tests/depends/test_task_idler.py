import asyncio
from asyncio.exceptions import CancelledError

import anyio
import pytest
from taskiq_dependencies import Depends

from taskiq.api.receiver import run_receiver_task
from taskiq.brokers.inmemory_broker import InmemoryResultBackend
from taskiq.depends.task_idler import TaskIdler
from tests.utils import AsyncQueueBroker


@pytest.mark.anyio
async def test_task_idler() -> None:
    broker = AsyncQueueBroker().with_result_backend(InmemoryResultBackend())
    kicked = 0
    desired_kicked = 20

    @broker.task(timeout=1)
    async def test_func(idle: TaskIdler = Depends()) -> None:
        nonlocal kicked
        async with idle():
            await asyncio.sleep(0.5)
        kicked += 1

    receiver_task = asyncio.create_task(run_receiver_task(broker, max_async_tasks=1))

    tasks = []
    for _ in range(desired_kicked):
        tasks.append(await test_func.kiq())

    with anyio.fail_after(1):
        for task in tasks:
            await task.wait_result(check_interval=0.01)

    receiver_task.cancel()
    assert kicked == desired_kicked


@pytest.mark.anyio
async def test_task_idler_task_cancelled() -> None:
    broker = AsyncQueueBroker().with_result_backend(InmemoryResultBackend())
    kicked = 0
    desired_kicked = 20

    @broker.task(timeout=0.2)
    async def test_func_timeout(idle: TaskIdler = Depends()) -> None:
        nonlocal kicked
        try:
            async with idle():
                await asyncio.sleep(2)
        except CancelledError:
            kicked += 1
            raise

    @broker.task(timeout=2)
    async def test_func(idle: TaskIdler = Depends()) -> None:
        nonlocal kicked
        async with idle():
            await asyncio.sleep(0.5)
        kicked += 1

    receiver_task = asyncio.create_task(run_receiver_task(broker, max_async_tasks=1))

    tasks = []
    tasks.append(await test_func_timeout.kiq())
    for _ in range(desired_kicked):
        tasks.append(await test_func.kiq())

    with anyio.fail_after(1):
        for task in tasks:
            await task.wait_result(check_interval=0.01)

    receiver_task.cancel()
    assert kicked == desired_kicked + 1
