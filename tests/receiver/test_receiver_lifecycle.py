import asyncio
import logging
from unittest.mock import Mock

import anyio
import pytest

from taskiq.receiver.receiver import Receiver, _PrefetchedMessage, _QueueSignal
from tests.receiver.receiver_lifecycle_support import (
    ControlledReceiver,
    ReceiverQueue,
    ShieldCallCounter,
    assert_exact_capacity,
    start_callback,
    wait_for_signals,
)
from tests.receiver.receiver_listener_support import (
    ReceiverLifecycleError,
    assert_semaphore_capacity,
)
from tests.utils import AsyncQueueBroker

RUNNER_PROBE_TASK_NAME = "receiver-drain-cancellation-probe"


async def test_finite_timeout_cancels_and_drains_callback() -> None:
    receiver = ControlledReceiver(
        wait_tasks_timeout=0.01,
        max_async_tasks=None,
    )
    queue, runner_task = await start_callback(receiver)

    try:
        await queue.put(_QueueSignal.DONE)
        await asyncio.wait_for(queue.shutdown_received.wait(), timeout=1)
        await wait_for_signals(receiver.cleanup_started_callbacks)
        assert not runner_task.done()

        receiver.release_cleanup.set()
        await asyncio.wait_for(runner_task, timeout=1)

        assert receiver.finished_callbacks.qsize() == 1
        assert receiver.callback_task is not None
        assert receiver.callback_task.cancelled()
    finally:
        await receiver.settle(runner_task)


async def test_finite_timeout_drains_every_active_callback() -> None:
    receiver = ControlledReceiver(
        wait_tasks_timeout=0.01,
        max_async_tasks=None,
    )
    queue = ReceiverQueue()
    await queue.put(_PrefetchedMessage(b"first", owns_delivery_slot=False))
    await queue.put(_PrefetchedMessage(b"second", owns_delivery_slot=False))
    runner_task = asyncio.create_task(receiver.runner(queue))

    try:
        await wait_for_signals(receiver.started_callbacks, count=2)
        await queue.put(_QueueSignal.DONE)
        await asyncio.wait_for(queue.shutdown_received.wait(), timeout=1)
        await wait_for_signals(receiver.cleanup_started_callbacks, count=2)
        assert not runner_task.done()

        receiver.release_cleanup.set()
        await asyncio.wait_for(runner_task, timeout=1)

        assert len(receiver.callback_tasks) == 2
        assert all(task.cancelled() for task in receiver.callback_tasks)
        assert receiver.finished_callbacks.qsize() == 2
    finally:
        await receiver.settle(runner_task)


async def test_outer_cancellation_drains_callback_before_runner_stops() -> None:
    receiver = ControlledReceiver(wait_tasks_timeout=None)
    _, runner_task = await start_callback(receiver)

    try:
        runner_task.cancel()
        await wait_for_signals(receiver.cleanup_started_callbacks)
        assert not runner_task.done()

        receiver.release_cleanup.set()
        await asyncio.wait_for(runner_task, timeout=1)

        assert receiver.finished_callbacks.qsize() == 1
    finally:
        await receiver.settle(runner_task)


async def test_cancellation_during_graceful_wait_drains_callback() -> None:
    receiver = ControlledReceiver(
        wait_tasks_timeout=None,
        max_async_tasks=None,
    )
    queue, runner_task = await start_callback(receiver)

    try:
        await queue.put(_QueueSignal.DONE)
        await asyncio.wait_for(queue.shutdown_received.wait(), timeout=1)
        assert not runner_task.done()

        runner_task.cancel()
        await wait_for_signals(receiver.cleanup_started_callbacks)
        receiver.release_cleanup.set()

        await asyncio.wait_for(runner_task, timeout=1)

        assert receiver.finished_callbacks.qsize() == 1
    finally:
        await receiver.settle(runner_task)


async def test_repeated_cancellation_does_not_interrupt_callback_cleanup() -> None:
    receiver = ControlledReceiver(wait_tasks_timeout=None)
    _, runner_task = await start_callback(receiver)

    try:
        runner_task.cancel()
        await wait_for_signals(receiver.cleanup_started_callbacks)
        runner_task.cancel()
        receiver.release_cleanup.set()

        await asyncio.wait_for(runner_task, timeout=1)

        assert receiver.finished_callbacks.qsize() == 1
    finally:
        await receiver.settle(runner_task)


@pytest.mark.parametrize("stop", ["timeout", "cancel"])
@pytest.mark.parametrize("max_prefetch", [0, 1])
async def test_listen_shutdown_preserves_multicheckpoint_task_cleanup(
    stop: str,
    max_prefetch: int,
) -> None:
    broker = AsyncQueueBroker()
    callback_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()
    callback_task: asyncio.Task[None] | None = None

    @broker.task
    async def blocked_task() -> None:
        nonlocal callback_task
        callback_task = asyncio.current_task()
        callback_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await release_cleanup.wait()
            for _ in range(20):
                await asyncio.sleep(0)
            cleanup_finished.set()

    await blocked_task.kiq()
    receiver = Receiver(
        broker,
        max_async_tasks=1,
        max_prefetch=max_prefetch,
        run_startup=False,
        wait_tasks_timeout=0.01,
    )
    finish_event = asyncio.Event()
    listen_task = asyncio.create_task(receiver.listen(finish_event))

    try:
        await asyncio.wait_for(callback_started.wait(), timeout=1)
        if stop == "timeout":
            finish_event.set()
        else:
            listen_task.cancel()
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        assert not listen_task.done()
        if stop == "cancel":
            listen_task.cancel()
        release_cleanup.set()
        await asyncio.gather(listen_task, return_exceptions=True)

        assert cleanup_finished.is_set()
        await assert_exact_capacity(receiver)
        await assert_semaphore_capacity(receiver.sem_prefetch, max_prefetch + 1)
    finally:
        release_cleanup.set()
        listen_task.cancel()
        if callback_task is not None and not callback_task.done():
            callback_task.cancel()
        await asyncio.gather(
            *(task for task in (callback_task, listen_task) if task is not None),
            return_exceptions=True,
        )


async def test_level_cancellation_does_not_busy_spin_during_callback_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receiver = ControlledReceiver(wait_tasks_timeout=None)
    queue = ReceiverQueue()
    await queue.put(_PrefetchedMessage(b"payload", owns_delivery_slot=False))
    shield_counter = ShieldCallCounter(RUNNER_PROBE_TASK_NAME)
    monkeypatch.setattr(asyncio, "shield", shield_counter)

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(
            receiver.runner,
            queue,
            name=RUNNER_PROBE_TASK_NAME,
        )
        await wait_for_signals(receiver.started_callbacks)
        task_group.cancel_scope.cancel()

        with anyio.CancelScope(shield=True):
            try:
                await wait_for_signals(receiver.cleanup_started_callbacks)
                for _ in range(20):
                    await asyncio.sleep(0)
            finally:
                receiver.release_cleanup.set()

    assert receiver.finished_callbacks.qsize() == 1
    assert shield_counter.runner_calls <= 2


async def test_level_cancellation_interrupts_unbounded_graceful_wait() -> None:
    receiver = ControlledReceiver(
        wait_tasks_timeout=None,
        max_async_tasks=None,
    )
    queue = ReceiverQueue()
    await queue.put(_PrefetchedMessage(b"payload", owns_delivery_slot=False))

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(receiver.runner, queue)
        await wait_for_signals(receiver.started_callbacks)
        await queue.put(_QueueSignal.DONE)
        await asyncio.wait_for(queue.shutdown_received.wait(), timeout=1)
        task_group.cancel_scope.cancel()

        with anyio.CancelScope(shield=True):
            try:
                await wait_for_signals(receiver.cleanup_started_callbacks)
            finally:
                receiver.release_callback.set()
                receiver.release_cleanup.set()

    assert receiver.callback_task is not None
    assert receiver.callback_task.cancelled()


async def test_unbounded_wait_preserves_graceful_callback_completion() -> None:
    receiver = ControlledReceiver(
        wait_tasks_timeout=None,
        max_async_tasks=None,
    )
    queue, runner_task = await start_callback(receiver)

    try:
        await queue.put(_QueueSignal.DONE)
        await asyncio.wait_for(queue.shutdown_received.wait(), timeout=1)
        assert not runner_task.done()

        receiver.release_callback.set()
        receiver.release_cleanup.set()
        await asyncio.wait_for(runner_task, timeout=1)

        assert receiver.finished_callbacks.qsize() == 1
        assert receiver.callback_task is not None
        assert not receiver.callback_task.cancelled()
    finally:
        await receiver.settle(runner_task)


async def test_runner_cancellation_before_message_releases_capacity() -> None:
    receiver = ControlledReceiver(wait_tasks_timeout=None)
    queue = ReceiverQueue()
    runner_task = asyncio.create_task(receiver.runner(queue))

    try:
        await asyncio.wait_for(queue.get_started.wait(), timeout=1)
        assert receiver.sem is not None
        assert not receiver.sem.locked()

        runner_task.cancel()
        await asyncio.gather(runner_task, return_exceptions=True)

        await assert_exact_capacity(receiver)
    finally:
        await receiver.settle(runner_task)


async def test_runner_failure_drains_active_callback_and_releases_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receiver = ControlledReceiver(
        wait_tasks_timeout=None,
        max_async_tasks=2,
    )
    queue = ReceiverQueue()
    await queue.put(_PrefetchedMessage(b"first", owns_delivery_slot=False))
    runner_task = asyncio.create_task(receiver.runner(queue))

    try:
        await wait_for_signals(receiver.started_callbacks)
        monkeypatch.setattr(
            receiver,
            "_run_owned_callback",
            Mock(side_effect=ReceiverLifecycleError("callback handoff failed")),
        )
        await queue.put(_PrefetchedMessage(b"second", owns_delivery_slot=False))
        await wait_for_signals(receiver.cleanup_started_callbacks)
        assert not runner_task.done()

        receiver.release_cleanup.set()
        with pytest.raises(
            ReceiverLifecycleError,
            match="callback handoff failed",
        ):
            await asyncio.wait_for(runner_task, timeout=1)

        assert receiver.callback_task is not None
        assert receiver.callback_task.cancelled()
        assert receiver.finished_callbacks.qsize() == 1
        await assert_exact_capacity(receiver, slots=2)
    finally:
        await receiver.settle(runner_task)


async def test_callback_completion_releases_exactly_one_capacity_slot() -> None:
    receiver = ControlledReceiver(wait_tasks_timeout=None)
    queue, runner_task = await start_callback(receiver)

    try:
        await queue.put(_QueueSignal.DONE)
        receiver.release_callback.set()
        receiver.release_cleanup.set()
        await asyncio.wait_for(runner_task, timeout=1)

        await assert_exact_capacity(receiver)
    finally:
        await receiver.settle(runner_task)


async def test_handoff_failure_drains_callback_before_its_first_step(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receiver = ControlledReceiver(wait_tasks_timeout=None, max_async_tasks=2)
    queue = ReceiverQueue()
    queue.put_nowait(_PrefetchedMessage(b"first", owns_delivery_slot=False))
    queue.put_nowait(_PrefetchedMessage(b"second", owns_delivery_slot=False))
    first_callback = receiver._run_owned_callback(message=b"first")
    monkeypatch.setattr(
        receiver,
        "_run_owned_callback",
        Mock(
            side_effect=[
                first_callback,
                ReceiverLifecycleError("callback handoff failed"),
            ],
        ),
    )

    with pytest.raises(ReceiverLifecycleError, match="callback handoff failed"):
        await receiver.runner(queue)

    assert receiver.started_callbacks.empty()
    await assert_exact_capacity(receiver, slots=2)


async def test_callback_failure_is_logged_and_releases_capacity(
    caplog: pytest.LogCaptureFixture,
) -> None:
    receiver = ControlledReceiver(wait_tasks_timeout=None, fail=True)
    queue, runner_task = await start_callback(receiver)
    caplog.set_level(logging.ERROR, logger="taskiq.receiver.receiver")

    try:
        await queue.put(_QueueSignal.DONE)
        receiver.release_callback.set()
        receiver.release_cleanup.set()
        await asyncio.wait_for(runner_task, timeout=1)

        await assert_exact_capacity(receiver)
        assert "Receiver callback failed outside task execution handling" in caplog.text
    finally:
        await receiver.settle(runner_task)
