import asyncio
import contextlib
from typing import Any

import pytest

from taskiq.api import run_receiver_task
from taskiq.receiver import Receiver
from tests.utils import AsyncQueueBroker


class _UnexpectedReceiverRetry(BaseException):
    """Signal that invalid configuration reached the Receiver retry loop."""


class _ValidationProbeReceiver(Receiver):
    """Fail deterministically if invalid configuration is retried."""

    construction_attempts = 0

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        type(self).construction_attempts += 1
        if type(self).construction_attempts > 1:
            raise _UnexpectedReceiverRetry
        super().__init__(*args, **kwargs)


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


async def test_negative_prefetch_is_rejected_before_receiver_retry() -> None:
    broker = AsyncQueueBroker()
    _ValidationProbeReceiver.construction_attempts = 0

    with pytest.raises(ValueError, match="max_prefetch cannot be negative"):
        await run_receiver_task(
            broker,
            receiver_cls=_ValidationProbeReceiver,
            max_prefetch=-1,
        )

    assert _ValidationProbeReceiver.construction_attempts == 0
    assert not broker.is_worker_process
