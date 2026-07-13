import asyncio
import logging

import anyio
import pytest

from taskiq.abc.broker import AckableMessage
from taskiq.receiver.receiver import QUEUE_DONE, Receiver
from tests.receiver.receiver_lifecycle_support import (
    BlockingRemoveMiddleware,
    ControlledReceiver,
    FailingRemoveMiddleware,
    FailingSecondRemoveMiddleware,
    ReceiverLifecycleError,
    ReceiverQueue,
    ShieldCallCounter,
    assert_exact_capacity,
    start_callback,
    wait_for_signals,
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
        await queue.put(QUEUE_DONE)
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
    await queue.put(b"first")
    await queue.put(b"second")
    runner_task = asyncio.create_task(receiver.runner(queue))

    try:
        await wait_for_signals(receiver.started_callbacks, count=2)
        await queue.put(QUEUE_DONE)
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
        await queue.put(QUEUE_DONE)
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


async def test_listen_cancellation_preserves_multicheckpoint_task_cleanup() -> None:
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
        run_startup=False,
        wait_tasks_timeout=None,
    )
    listen_task = asyncio.create_task(receiver.listen(asyncio.Event()))

    try:
        await asyncio.wait_for(callback_started.wait(), timeout=1)
        listen_task.cancel()
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        listen_task.cancel()
        release_cleanup.set()
        await asyncio.gather(listen_task, return_exceptions=True)

        assert cleanup_finished.is_set()
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
    await queue.put(b"payload")
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
    await queue.put(b"payload")

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(receiver.runner, queue)
        await wait_for_signals(receiver.started_callbacks)
        await queue.put(QUEUE_DONE)
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
        await queue.put(QUEUE_DONE)
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
        assert receiver.sem.locked()

        runner_task.cancel()
        await asyncio.gather(runner_task, return_exceptions=True)

        await assert_exact_capacity(receiver)
    finally:
        await receiver.settle(runner_task)


async def test_prefetch_remove_failure_releases_capacity() -> None:
    receiver = ControlledReceiver(wait_tasks_timeout=None)
    receiver.broker.with_middlewares(FailingRemoveMiddleware())
    queue: asyncio.Queue[bytes | AckableMessage] = asyncio.Queue()
    await queue.put(b"payload")

    with pytest.raises(ReceiverLifecycleError, match="prefetch remove failed"):
        await receiver.runner(queue)

    await assert_exact_capacity(receiver)


async def test_prefetch_remove_cancellation_releases_capacity() -> None:
    receiver = ControlledReceiver(wait_tasks_timeout=None)
    middleware = BlockingRemoveMiddleware()
    receiver.broker.with_middlewares(middleware)
    queue: asyncio.Queue[bytes | AckableMessage] = asyncio.Queue()
    await queue.put(b"payload")
    runner_task = asyncio.create_task(receiver.runner(queue))

    try:
        await asyncio.wait_for(middleware.started.wait(), timeout=1)
        runner_task.cancel()
        await asyncio.gather(runner_task, return_exceptions=True)

        assert receiver.started_callbacks.empty()
        await assert_exact_capacity(receiver)
    finally:
        await receiver.settle(runner_task)


async def test_runner_failure_drains_active_callback_and_releases_capacity() -> None:
    receiver = ControlledReceiver(
        wait_tasks_timeout=None,
        max_async_tasks=2,
    )
    receiver.broker.with_middlewares(FailingSecondRemoveMiddleware())
    queue = ReceiverQueue()
    await queue.put(b"first")
    runner_task = asyncio.create_task(receiver.runner(queue))

    try:
        await wait_for_signals(receiver.started_callbacks)
        await queue.put(b"second")
        await wait_for_signals(receiver.cleanup_started_callbacks)
        assert not runner_task.done()

        receiver.release_cleanup.set()
        with pytest.raises(
            ReceiverLifecycleError,
            match="second prefetch remove failed",
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
        await queue.put(QUEUE_DONE)
        receiver.release_callback.set()
        receiver.release_cleanup.set()
        await asyncio.wait_for(runner_task, timeout=1)

        await assert_exact_capacity(receiver)
    finally:
        await receiver.settle(runner_task)


async def test_callback_failure_is_logged_and_releases_capacity(
    caplog: pytest.LogCaptureFixture,
) -> None:
    receiver = ControlledReceiver(wait_tasks_timeout=None, fail=True)
    queue, runner_task = await start_callback(receiver)
    caplog.set_level(logging.ERROR, logger="taskiq.receiver.receiver")

    try:
        await queue.put(QUEUE_DONE)
        receiver.release_callback.set()
        receiver.release_cleanup.set()
        await asyncio.wait_for(runner_task, timeout=1)

        await assert_exact_capacity(receiver)
        assert "Receiver callback failed outside task execution handling" in caplog.text
    finally:
        await receiver.settle(runner_task)
