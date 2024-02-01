import asyncio
import contextlib

import pytest

from taskiq.api import run_receiver_task
from tests.utils import AsyncQueueBroker


@pytest.mark.anyio
async def test_successful() -> None:
    broker = AsyncQueueBroker()
    kicked = 0
    desired_kicked = 3

    @broker.task
    def test_func() -> None:
        nonlocal kicked
        kicked += 1

    receiver_task = asyncio.create_task(run_receiver_task(broker))

    for _ in range(desired_kicked):
        await test_func.kiq()

    await broker.wait_tasks()
    receiver_task.cancel()
    assert kicked == desired_kicked


@pytest.mark.anyio
async def test_cancelation() -> None:
    broker = AsyncQueueBroker()
    kicked = 0

    @broker.task
    def test_func() -> None:
        nonlocal kicked
        kicked += 1

    receiver_task = asyncio.create_task(run_receiver_task(broker))

    await test_func.kiq()
    await broker.wait_tasks()
    assert kicked == 1

    receiver_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await receiver_task

    assert receiver_task.cancelled()

    await test_func.kiq()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(broker.wait_tasks(), 0.2)
    assert kicked == 1
