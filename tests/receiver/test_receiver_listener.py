import asyncio
import logging
import unittest.mock
from collections.abc import AsyncGenerator

import pytest

from taskiq.abc.broker import AckableMessage
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.receiver import Receiver
from tests.receiver.receiver_listener_support import (
    ControlledBroker,
    ListenerBroker,
    ObservedSemaphore,
    PrefetchCounterMiddleware,
    ReceiverLifecycleError,
    assert_semaphore_capacity,
    contains_exception,
    encoded_message,
)


@pytest.mark.parametrize("explicit_zero", [False, True])
async def test_default_and_zero_prefetch_make_progress(explicit_zero: bool) -> None:
    broker = ControlledBroker()
    executed = asyncio.Event()

    @broker.task(task_name=f"receiver.prefetch.progress.{explicit_zero}")
    async def task() -> None:
        executed.set()

    if explicit_zero:
        receiver = Receiver(
            broker,
            max_async_tasks=1,
            max_prefetch=0,
            run_startup=False,
        )
    else:
        receiver = Receiver(
            broker,
            max_async_tasks=1,
            run_startup=False,
        )
    finish_event = asyncio.Event()
    await task.kiq()
    listen_task = asyncio.create_task(receiver.listen(finish_event))

    try:
        await asyncio.wait_for(executed.wait(), timeout=1)
    finally:
        finish_event.set()
        await asyncio.wait_for(listen_task, timeout=1)

    assert broker.closed.is_set()


async def test_zero_prefetch_does_not_read_ahead_of_saturated_execution() -> None:
    broker = ControlledBroker()
    callback_started = asyncio.Event()
    callback_finished = asyncio.Event()
    release_callback = asyncio.Event()

    @broker.task(task_name="receiver.prefetch.zero-buffer")
    async def task() -> None:
        callback_started.set()
        try:
            await release_callback.wait()
        finally:
            callback_finished.set()

    receiver = Receiver(
        broker,
        max_async_tasks=1,
        max_prefetch=0,
        run_startup=False,
        wait_tasks_timeout=0,
    )
    delivery_capacity = ObservedSemaphore(1)
    receiver.sem_prefetch = delivery_capacity
    for _ in range(3):
        await task.kiq()

    finish_event = asyncio.Event()
    listen_task = asyncio.create_task(receiver.listen(finish_event))
    assert await delivery_capacity.acquire_attempts.get() == 1
    assert await broker.read_started.get() == 1
    await asyncio.wait_for(callback_started.wait(), timeout=1)
    assert await delivery_capacity.acquire_attempts.get() == 2

    assert broker.read_started.empty()
    assert broker.incoming.qsize() == 2
    assert delivery_capacity.locked()

    finish_event.set()
    await asyncio.wait_for(listen_task, timeout=1)
    release_callback.set()
    await asyncio.wait_for(callback_finished.wait(), timeout=1)

    await assert_semaphore_capacity(delivery_capacity, 1)
    assert receiver.sem is not None
    await assert_semaphore_capacity(receiver.sem, 1)


async def test_zero_prefetch_preserves_bounded_execution_concurrency() -> None:
    broker = ControlledBroker()
    two_callbacks_started = asyncio.Event()
    callbacks_finished = asyncio.Event()
    release_callbacks = asyncio.Event()
    started = 0
    finished = 0

    @broker.task(task_name="receiver.prefetch.bounded-concurrency")
    async def task() -> None:
        nonlocal finished, started
        started += 1
        if started == 2:
            two_callbacks_started.set()
        try:
            await release_callbacks.wait()
        finally:
            finished += 1
            if finished == 2:
                callbacks_finished.set()

    receiver = Receiver(
        broker,
        max_async_tasks=2,
        max_prefetch=0,
        run_startup=False,
        wait_tasks_timeout=0,
    )
    delivery_capacity = ObservedSemaphore(2)
    receiver.sem_prefetch = delivery_capacity
    for _ in range(3):
        await task.kiq()

    finish_event = asyncio.Event()
    listen_task = asyncio.create_task(receiver.listen(finish_event))
    assert [await delivery_capacity.acquire_attempts.get() for _ in range(3)] == [
        1,
        2,
        3,
    ]
    assert [await broker.read_started.get() for _ in range(2)] == [1, 2]
    await asyncio.wait_for(two_callbacks_started.wait(), timeout=1)

    assert broker.read_started.empty()
    assert broker.incoming.qsize() == 1
    assert delivery_capacity.locked()

    finish_event.set()
    await asyncio.wait_for(listen_task, timeout=1)
    release_callbacks.set()
    await asyncio.wait_for(callbacks_finished.wait(), timeout=1)

    await assert_semaphore_capacity(delivery_capacity, 2)
    assert receiver.sem is not None
    await assert_semaphore_capacity(receiver.sem, 2)


async def test_prefetch_is_additional_to_bounded_execution_capacity() -> None:
    broker = ControlledBroker()
    two_callbacks_started = asyncio.Event()
    three_callbacks_finished = asyncio.Event()
    release_callbacks = asyncio.Event()
    started = 0
    finished = 0

    @broker.task(task_name="receiver.prefetch.additional-capacity")
    async def task() -> None:
        nonlocal finished, started
        started += 1
        if started == 2:
            two_callbacks_started.set()
        await release_callbacks.wait()
        finished += 1
        if finished == 3:
            three_callbacks_finished.set()

    receiver = Receiver(
        broker,
        max_async_tasks=2,
        max_prefetch=1,
        run_startup=False,
    )
    delivery_capacity = ObservedSemaphore(3)
    receiver.sem_prefetch = delivery_capacity
    for _ in range(4):
        await task.kiq()

    finish_event = asyncio.Event()
    listen_task = asyncio.create_task(receiver.listen(finish_event))
    assert [await delivery_capacity.acquire_attempts.get() for _ in range(4)] == [
        1,
        2,
        3,
        4,
    ]
    assert [await broker.read_started.get() for _ in range(3)] == [1, 2, 3]
    await asyncio.wait_for(two_callbacks_started.wait(), timeout=1)

    assert broker.read_started.empty()
    assert broker.incoming.qsize() == 1
    assert delivery_capacity.locked()

    finish_event.set()
    release_callbacks.set()
    await asyncio.wait_for(listen_task, timeout=1)
    await asyncio.wait_for(three_callbacks_finished.wait(), timeout=1)

    assert started == 3
    await assert_semaphore_capacity(delivery_capacity, 3)
    assert receiver.sem is not None
    await assert_semaphore_capacity(receiver.sem, 2)


async def test_unlimited_execution_keeps_zero_prefetch_handoff_progress() -> None:
    broker = ControlledBroker()
    two_callbacks_started = asyncio.Event()
    callbacks_finished = asyncio.Event()
    release_callbacks = asyncio.Event()
    finished = 0
    started = 0

    @broker.task(task_name="receiver.prefetch.unlimited-concurrency")
    async def task() -> None:
        nonlocal finished, started
        started += 1
        if started == 2:
            two_callbacks_started.set()
        try:
            await release_callbacks.wait()
        finally:
            finished += 1
            if finished == 2:
                callbacks_finished.set()

    receiver = Receiver(
        broker,
        max_prefetch=0,
        run_startup=False,
        wait_tasks_timeout=0,
    )
    await task.kiq()
    await task.kiq()

    finish_event = asyncio.Event()
    listen_task = asyncio.create_task(receiver.listen(finish_event))
    await asyncio.wait_for(two_callbacks_started.wait(), timeout=1)

    finish_event.set()
    await asyncio.wait_for(listen_task, timeout=1)
    release_callbacks.set()
    await asyncio.wait_for(callbacks_finished.wait(), timeout=1)


def test_delivery_capacity_uses_jittered_execution_limit() -> None:
    with unittest.mock.patch(
        "taskiq.receiver.receiver.random.randint",
        return_value=3,
    ):
        receiver = Receiver(
            ControlledBroker(),
            max_async_tasks=5,
            max_async_tasks_jitter=4,
            max_prefetch=2,
            run_startup=False,
        )

    assert receiver.sem is not None
    assert receiver.sem._value == 8
    assert receiver.sem_prefetch._value == 10


def test_negative_prefetch_is_rejected_before_listener_startup() -> None:
    broker = ControlledBroker()

    with pytest.raises(ValueError, match="max_prefetch cannot be negative"):
        Receiver(broker, max_prefetch=-1, run_startup=False)

    assert broker.listen_calls == 0


async def test_finish_wakes_prefetcher_blocked_on_capacity() -> None:
    broker = ControlledBroker()
    receiver = Receiver(broker, max_prefetch=0, run_startup=False)
    observed_semaphore = ObservedSemaphore(0)
    receiver.sem_prefetch = observed_semaphore
    finish_event = asyncio.Event()
    listen_task = asyncio.create_task(receiver.listen(finish_event))

    await observed_semaphore.acquire_started.wait()
    finish_event.set()
    await asyncio.wait_for(listen_task, timeout=1)

    assert observed_semaphore.locked()


async def test_pending_read_is_cancelled_and_iterator_closed() -> None:
    broker = ControlledBroker()
    receiver = Receiver(broker, max_prefetch=1, run_startup=False)
    finish_event = asyncio.Event()
    listen_task = asyncio.create_task(receiver.listen(finish_event))

    assert await broker.read_started.get() == 1
    finish_event.set()
    await asyncio.wait_for(listen_task, timeout=1)

    assert broker.closed.is_set()
    await assert_semaphore_capacity(receiver.sem_prefetch, 2)


async def test_receiver_cancellation_closes_pending_read() -> None:
    broker = ControlledBroker()
    receiver = Receiver(broker, max_prefetch=0, run_startup=False)
    listen_task = asyncio.create_task(receiver.listen(asyncio.Event()))

    assert await broker.read_started.get() == 1
    listen_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await listen_task

    assert broker.closed.is_set()
    await assert_semaphore_capacity(receiver.sem_prefetch, 1)


async def test_ready_delivery_wins_concurrent_finish_signal() -> None:
    broker = ControlledBroker()
    executed = asyncio.Event()

    @broker.task(task_name="receiver.prefetch.ready-at-finish")
    async def task() -> None:
        executed.set()

    receiver = Receiver(
        broker,
        max_async_tasks=1,
        max_prefetch=0,
        run_startup=False,
    )
    finish_event = asyncio.Event()
    listen_task = asyncio.create_task(receiver.listen(finish_event))
    assert await broker.read_started.get() == 1

    broker.incoming.put_nowait(encoded_message(broker, task.task_name))
    finish_event.set()

    await asyncio.wait_for(executed.wait(), timeout=1)
    await asyncio.wait_for(listen_task, timeout=1)


async def test_late_delivery_is_processed_and_hook_failure_propagates() -> None:
    read_started = asyncio.Event()
    finish_event = asyncio.Event()
    executed = asyncio.Event()
    hook_error = ReceiverLifecycleError("late prefetch hook failed")
    prefetch_counter = PrefetchCounterMiddleware()

    async def cancellation_delivery() -> AsyncGenerator[bytes | AckableMessage, None]:
        read_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            yield encoded_message(broker, task.task_name)

    class FailingAddMiddleware(TaskiqMiddleware):
        def on_prefetch_queue_add(self) -> None:
            raise hook_error

    broker = ListenerBroker(cancellation_delivery).with_middlewares(
        prefetch_counter,
        FailingAddMiddleware(),
    )

    @broker.task(task_name="receiver.prefetch.cancellation-delivery")
    async def task() -> None:
        executed.set()

    receiver = Receiver(
        broker,
        max_async_tasks=1,
        max_prefetch=0,
        run_startup=False,
    )
    listen_task = asyncio.create_task(receiver.listen(finish_event))
    await read_started.wait()

    finish_event.set()
    with pytest.raises(ReceiverLifecycleError) as exc_info:
        await asyncio.wait_for(listen_task, timeout=1)

    assert exc_info.value is hook_error
    assert executed.is_set()
    assert prefetch_counter.queued_messages == 0


async def test_pending_read_failure_during_shutdown_is_propagated() -> None:
    read_started = asyncio.Event()
    finish_event = asyncio.Event()
    read_error = ReceiverLifecycleError("pending transport read failed")

    async def cancellation_failure() -> AsyncGenerator[bytes | AckableMessage, None]:
        read_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise read_error from None
        if False:  # pragma: no branch
            yield b""

    broker = ListenerBroker(cancellation_failure)
    receiver = Receiver(broker, max_prefetch=0, run_startup=False)
    listen_task = asyncio.create_task(receiver.listen(finish_event))
    await read_started.wait()

    finish_event.set()
    with pytest.raises(ReceiverLifecycleError) as exc_info:
        await asyncio.wait_for(listen_task, timeout=1)

    assert exc_info.value is read_error


async def test_iterator_close_failure_is_propagated_after_delivery() -> None:
    finish_event = asyncio.Event()
    executed = asyncio.Event()
    close_error = ReceiverLifecycleError("listener close failed")

    async def close_failure() -> AsyncGenerator[bytes | AckableMessage, None]:
        try:
            yield encoded_message(broker, task.task_name)
            await asyncio.Event().wait()
        finally:
            raise close_error

    class StopAfterPrefetchMiddleware(TaskiqMiddleware):
        def on_prefetch_queue_add(self) -> None:
            finish_event.set()

    broker = ListenerBroker(close_failure).with_middlewares(
        StopAfterPrefetchMiddleware(),
    )

    @broker.task(task_name="receiver.prefetch.close-failure")
    async def task() -> None:
        executed.set()

    receiver = Receiver(
        broker,
        max_async_tasks=1,
        max_prefetch=0,
        run_startup=False,
    )

    with pytest.raises(ReceiverLifecycleError) as exc_info:
        await asyncio.wait_for(receiver.listen(finish_event), timeout=1)

    assert exc_info.value is close_error
    assert executed.is_set()


async def test_listener_failure_propagates_and_releases_capacity() -> None:
    broker = ControlledBroker()
    listener_error = ReceiverLifecycleError("listener failed")
    receiver = Receiver(broker, max_prefetch=1, run_startup=False)
    await broker.incoming.put(listener_error)

    with pytest.raises(BaseException) as exc_info:
        await asyncio.wait_for(receiver.listen(asyncio.Event()), timeout=1)

    assert contains_exception(exc_info.value, listener_error)
    assert broker.closed.is_set()
    await assert_semaphore_capacity(receiver.sem_prefetch, 2)


async def test_listener_failure_interrupts_running_task_wait() -> None:
    broker = ControlledBroker()
    listener_error = ReceiverLifecycleError("listener failed while task was running")
    task_started = asyncio.Event()
    task_finished = asyncio.Event()
    release_task = asyncio.Event()

    @broker.task(task_name="receiver.prefetch.listener-failure-running-task")
    async def task() -> None:
        task_started.set()
        try:
            await release_task.wait()
        finally:
            task_finished.set()

    receiver = Receiver(
        broker,
        max_async_tasks=1,
        max_prefetch=1,
        run_startup=False,
    )
    await task.kiq()
    listen_task = asyncio.create_task(receiver.listen(asyncio.Event()))
    await asyncio.wait_for(task_started.wait(), timeout=1)
    await broker.incoming.put(listener_error)

    try:
        with pytest.raises(BaseException) as exc_info:
            await asyncio.wait_for(asyncio.shield(listen_task), timeout=1)
    finally:
        release_task.set()
        await asyncio.wait_for(task_finished.wait(), timeout=1)
        await asyncio.gather(listen_task, return_exceptions=True)

    assert contains_exception(exc_info.value, listener_error)
    await assert_semaphore_capacity(receiver.sem_prefetch, 2)
    assert receiver.sem is not None
    await assert_semaphore_capacity(receiver.sem, 1)


async def test_recorded_listener_error_is_logged_when_runner_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    close_error = ReceiverLifecycleError("listener close failed after runner")
    hook_error = ReceiverLifecycleError("prefetch remove hook failed")

    async def close_failure() -> AsyncGenerator[bytes | AckableMessage, None]:
        try:
            yield encoded_message(broker, task.task_name)
            await asyncio.Event().wait()
        finally:
            raise close_error

    class FailingRemoveMiddleware(TaskiqMiddleware):
        def on_prefetch_queue_remove(self) -> None:
            raise hook_error

    broker = ListenerBroker(close_failure).with_middlewares(
        FailingRemoveMiddleware(),
    )

    @broker.task(task_name="receiver.prefetch.listener-and-runner-failure")
    async def task() -> None:
        pass

    receiver = Receiver(
        broker,
        max_async_tasks=1,
        max_prefetch=1,
        run_startup=False,
    )

    with (
        caplog.at_level(logging.ERROR, logger="taskiq.receiver.receiver"),
        pytest.raises(BaseException) as exc_info,
    ):
        await asyncio.wait_for(receiver.listen(asyncio.Event()), timeout=1)

    assert contains_exception(exc_info.value, hook_error)
    assert "A Receiver listener lifecycle error was recorded" in caplog.text
    assert str(close_error) in caplog.text


async def test_listener_open_failure_propagates() -> None:
    listener_error = ReceiverLifecycleError("listener open failed")

    def opening_failure() -> AsyncGenerator[bytes | AckableMessage, None]:
        raise listener_error

    broker = ListenerBroker(opening_failure)
    receiver = Receiver(broker, run_startup=False)

    with pytest.raises(ReceiverLifecycleError) as exc_info:
        await asyncio.wait_for(receiver.listen(asyncio.Event()), timeout=1)

    assert exc_info.value is listener_error


async def test_prefetch_add_hook_failure_preserves_delivery_and_capacity() -> None:
    hook_error = ReceiverLifecycleError("prefetch add hook failed")
    executed = asyncio.Event()
    prefetch_counter = PrefetchCounterMiddleware()

    class FailingAddMiddleware(TaskiqMiddleware):
        def on_prefetch_queue_add(self) -> None:
            raise hook_error

    broker = ControlledBroker().with_middlewares(
        prefetch_counter,
        FailingAddMiddleware(),
    )

    @broker.task(task_name="receiver.prefetch.add-failure")
    async def task() -> None:
        executed.set()

    receiver = Receiver(broker, max_prefetch=1, run_startup=False)
    await task.kiq()

    with pytest.raises(ReceiverLifecycleError) as exc_info:
        await asyncio.wait_for(receiver.listen(asyncio.Event()), timeout=1)

    assert exc_info.value is hook_error
    assert executed.is_set()
    assert broker.closed.is_set()
    assert prefetch_counter.queued_messages == 0
    await assert_semaphore_capacity(receiver.sem_prefetch, 2)


async def test_remove_hook_failure_releases_buffered_capacity(
    caplog: pytest.LogCaptureFixture,
) -> None:
    three_messages_added = asyncio.Event()
    hook_error = ReceiverLifecycleError("prefetch remove hook failed")
    task_started = asyncio.Event()
    added_messages = 0
    prefetch_counter = PrefetchCounterMiddleware()

    class CoordinatedFailingMiddleware(TaskiqMiddleware):
        def on_prefetch_queue_add(self) -> None:
            nonlocal added_messages
            added_messages += 1
            if added_messages == 3:
                three_messages_added.set()

        async def on_prefetch_queue_remove(self) -> None:
            await three_messages_added.wait()
            raise hook_error

    broker = ControlledBroker().with_middlewares(
        prefetch_counter,
        CoordinatedFailingMiddleware(),
    )

    @broker.task(task_name="receiver.prefetch.buffered-remove-failure")
    async def task() -> None:
        task_started.set()

    receiver = Receiver(
        broker,
        max_async_tasks=1,
        max_prefetch=2,
        run_startup=False,
    )
    for _ in range(3):
        await task.kiq()

    with (
        caplog.at_level(logging.WARNING, logger="taskiq.receiver.receiver"),
        pytest.raises(BaseException) as exc_info,
    ):
        await asyncio.wait_for(receiver.listen(asyncio.Event()), timeout=1)

    assert contains_exception(exc_info.value, hook_error)
    assert not task_started.is_set()
    assert "Discarding 2 prefetched deliveries during Receiver cleanup" in caplog.text
    assert prefetch_counter.queued_messages == 0
    await assert_semaphore_capacity(receiver.sem_prefetch, 3)
    assert receiver.sem is not None
    await assert_semaphore_capacity(receiver.sem, 1)


async def test_shutdown_sentinel_bypasses_saturated_execution_capacity() -> None:
    broker = ControlledBroker()
    callback_started = asyncio.Event()
    callback_finished = asyncio.Event()
    release_callback = asyncio.Event()

    @broker.task(task_name="receiver.prefetch.saturated-shutdown")
    async def task() -> None:
        callback_started.set()
        try:
            await release_callback.wait()
        finally:
            callback_finished.set()

    receiver = Receiver(
        broker,
        max_async_tasks=1,
        max_prefetch=0,
        run_startup=False,
        wait_tasks_timeout=0,
    )
    finish_event = asyncio.Event()
    await task.kiq()
    listen_task = asyncio.create_task(receiver.listen(finish_event))
    await asyncio.wait_for(callback_started.wait(), timeout=1)

    finish_event.set()
    try:
        await asyncio.wait_for(asyncio.shield(listen_task), timeout=1)
    finally:
        release_callback.set()
        await asyncio.wait_for(callback_finished.wait(), timeout=1)
        if not listen_task.done():
            await asyncio.wait_for(listen_task, timeout=1)

    assert receiver.sem is not None
    await assert_semaphore_capacity(receiver.sem, 1)
    assert broker.closed.is_set()


async def test_task_limit_leaves_the_next_delivery_in_the_broker() -> None:
    broker = ControlledBroker()
    executions = 0

    @broker.task(task_name="receiver.prefetch.legacy-limit")
    async def task() -> None:
        nonlocal executions
        executions += 1

    receiver = Receiver(
        broker,
        max_async_tasks=1,
        max_prefetch=1,
        max_tasks_to_execute=1,
        run_startup=False,
    )
    await task.kiq()
    await task.kiq()

    await asyncio.wait_for(receiver.listen(asyncio.Event()), timeout=1)

    assert executions == 1
    assert broker.incoming.qsize() == 1
