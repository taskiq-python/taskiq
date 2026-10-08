import asyncio
import contextlib
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock

import pytest

from taskiq.acks import AckableMessage
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


@pytest.mark.parametrize("failures", [0, 1, 3])
async def test_receiver_errors_delay_each_retry(
    monkeypatch: pytest.MonkeyPatch,
    failures: int,
) -> None:
    listen = AsyncMock(
        side_effect=[ConnectionError("Broker unavailable") for _ in range(failures)]
        + [asyncio.CancelledError()],
    )
    retry_delays: list[tuple[int, float]] = []

    async def record_sleep(delay: float) -> None:
        retry_delays.append((listen.await_count, delay))

    monkeypatch.setattr(Receiver, "listen", listen)
    monkeypatch.setattr(asyncio, "sleep", record_sleep)

    with pytest.raises(asyncio.CancelledError):
        await run_receiver_task(AsyncQueueBroker())

    assert retry_delays == [(attempt, 0.1) for attempt in range(1, failures + 1)]
    assert listen.await_count == failures + 1


async def test_cancellation_during_retry_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep_started = asyncio.Event()
    listen = AsyncMock(side_effect=[ConnectionError, asyncio.CancelledError])

    async def wait_for_cancellation(delay: float) -> None:
        sleep_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(Receiver, "listen", listen)
    monkeypatch.setattr(asyncio, "sleep", wait_for_cancellation)
    receiver_task = asyncio.create_task(run_receiver_task(AsyncQueueBroker()))
    try:
        await asyncio.wait_for(sleep_started.wait(), 1)
        receiver_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(receiver_task, 1)
        assert receiver_task.cancelled()
        assert listen.await_count == 1
    finally:
        receiver_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await receiver_task


async def test_receiver_recovers_after_broker_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = AsyncQueueBroker()
    original_listen = broker.listen
    attempts = 0
    executed = asyncio.Event()

    async def fail_once() -> AsyncGenerator[AckableMessage, None]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("Broker unavailable")
        async for message in original_listen():
            yield message

    @broker.task
    async def test_func() -> None:
        executed.set()

    monkeypatch.setattr(broker, "listen", fail_once)
    await test_func.kiq()
    receiver_task = asyncio.create_task(run_receiver_task(broker))
    try:
        await asyncio.wait_for(broker.wait_tasks(), 2)
        assert executed.is_set()
        assert attempts == 2
    finally:
        receiver_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await receiver_task


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
