import asyncio
import logging
from collections.abc import AsyncGenerator, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import cast

import pytest

from taskiq import BrokerMessage, TaskiqMessage
from taskiq.abc.broker import AckableMessage, AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.receiver import Receiver
from taskiq.receiver.receiver import _PrefetchedMessage
from taskiq.receiver.runtime import ReceiverRuntimeState


class ReceiverLifecycleError(RuntimeError):
    """Marker error for listener and middleware lifecycle failures."""


class ListenerBroker(AsyncBroker):
    """Broker adapter with a controllable listener implementation."""

    def __init__(self, listener_factory: Callable[[], object]) -> None:
        self.listener_factory = listener_factory
        super().__init__()

    async def kick(self, message: BrokerMessage) -> None:
        """Ignore outbound messages in listener-only tests."""

    def listen(self) -> AsyncGenerator[bytes, None]:
        """Return the configured listener object for Receiver validation."""
        return cast(AsyncGenerator[bytes, None], self.listener_factory())


def encoded_message(broker: AsyncBroker, task_name: str) -> bytes:
    """Build one valid transport payload for a registered task."""
    return broker.formatter.dumps(
        TaskiqMessage(
            task_id="task-id",
            task_name=task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    ).message


async def empty_listener() -> AsyncGenerator[bytes, None]:
    """Return a valid listener that reaches the end of its stream."""
    if False:  # pragma: no branch
        yield b""


async def test_receiver_accepts_finite_broker_listener() -> None:
    broker = ListenerBroker(empty_listener)
    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = Receiver(broker, executor=executor, run_startup=False)

        await receiver.listen(asyncio.Event())


async def test_single_receiver_stops_after_legacy_task_limit() -> None:
    executed = 0

    async def finite_listener() -> AsyncGenerator[bytes, None]:
        yield encoded_message(broker, "receiver.legacy-limit")
        yield encoded_message(broker, "receiver.legacy-limit")

    broker = ListenerBroker(finite_listener)

    @broker.task(task_name="receiver.legacy-limit")
    async def limited_task() -> None:
        nonlocal executed
        executed += 1

    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = Receiver(
            broker,
            executor=executor,
            run_startup=False,
            max_tasks_to_execute=1,
        )

        await receiver.listen(asyncio.Event())

    assert executed == 1


async def test_receiver_rejects_non_generator_listener() -> None:
    broker = ListenerBroker(lambda: ())
    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = Receiver(broker, executor=executor, run_startup=False)

        with pytest.raises(
            TypeError,
            match=r"Broker\.listen\(\) must return an async generator",
        ):
            await receiver.listen(asyncio.Event())


def test_receiver_rejects_negative_prefetch_limit() -> None:
    broker = ListenerBroker(empty_listener)
    with (
        ThreadPoolExecutor(max_workers=1) as executor,
        pytest.raises(
            ValueError,
            match="max_prefetch cannot be negative",
        ),
    ):
        Receiver(
            broker,
            executor=executor,
            run_startup=False,
            max_prefetch=-1,
        )


async def test_receiver_drains_delivery_completed_during_listener_stop() -> None:
    read_started = asyncio.Event()
    finish_event = asyncio.Event()
    executed = asyncio.Event()
    hook_error = ReceiverLifecycleError("late prefetch hook failed")

    async def late_listener() -> AsyncGenerator[bytes, None]:
        read_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            yield encoded_message(broker, "receiver.late-delivery")

    class FailingPrefetchMiddleware(TaskiqMiddleware):
        def on_prefetch_queue_add(self) -> None:
            raise hook_error

    broker = ListenerBroker(late_listener)
    broker.with_middlewares(FailingPrefetchMiddleware())

    @broker.task(task_name="receiver.late-delivery")
    async def late_delivery_task() -> None:
        executed.set()

    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = Receiver(broker, executor=executor, run_startup=False)
        runtime_state = ReceiverRuntimeState(
            finish_event=finish_event,
            max_async_tasks=1,
            max_async_tasks_jitter=0,
            max_prefetch=0,
            max_tasks_to_execute=None,
        )
        receiver.attach_runtime_state(runtime_state)
        listener_task = asyncio.create_task(receiver.listen(finish_event))
        await read_started.wait()

        finish_event.set()
        with pytest.raises(ReceiverLifecycleError) as exc_info:
            await listener_task

    assert exc_info.value is hook_error
    assert executed.is_set()
    assert runtime_state.fetched_tasks == 1


async def test_receiver_preserves_pending_read_failure_during_close() -> None:
    read_started = asyncio.Event()
    finish_event = asyncio.Event()
    read_error = ReceiverLifecycleError("pending transport read failed")

    async def failing_listener() -> AsyncGenerator[bytes, None]:
        read_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise read_error from None
        if False:  # pragma: no branch
            yield b""

    broker = ListenerBroker(failing_listener)
    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = Receiver(broker, executor=executor, run_startup=False)
        listener_task = asyncio.create_task(receiver.listen(finish_event))
        await read_started.wait()

        finish_event.set()
        with pytest.raises(ReceiverLifecycleError) as exc_info:
            await listener_task

    assert exc_info.value is read_error


async def test_receiver_preserves_iterator_close_failure_after_delivery() -> None:
    finish_event = asyncio.Event()
    executed = asyncio.Event()
    close_error = ReceiverLifecycleError("listener close failed")

    class StopAfterPrefetchMiddleware(TaskiqMiddleware):
        def on_prefetch_queue_add(self) -> None:
            finish_event.set()

    async def close_failing_listener() -> AsyncGenerator[bytes, None]:
        try:
            yield encoded_message(broker, "receiver.close-failure")
        finally:
            raise close_error

    broker = ListenerBroker(close_failing_listener)
    broker.with_middlewares(StopAfterPrefetchMiddleware())

    @broker.task(task_name="receiver.close-failure")
    async def delivered_task() -> None:
        executed.set()

    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = Receiver(broker, executor=executor, run_startup=False)

        with pytest.raises(ReceiverLifecycleError) as exc_info:
            await receiver.listen(finish_event)

    assert exc_info.value is close_error
    assert executed.is_set()


async def test_prefetch_hook_failure_releases_owned_capacity() -> None:
    hook_error = ReceiverLifecycleError("prefetch remove failed")

    class FailingRemoveMiddleware(TaskiqMiddleware):
        def on_prefetch_queue_remove(self) -> None:
            raise hook_error

    broker = ListenerBroker(empty_listener)
    broker.with_middlewares(FailingRemoveMiddleware())
    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = Receiver(
            broker,
            executor=executor,
            run_startup=False,
            max_prefetch=0,
        )
        await receiver.sem_prefetch.acquire()

        with pytest.raises(ReceiverLifecycleError) as exc_info:
            await receiver._start_callback(
                _PrefetchedMessage(data=b"message", owns_prefetch_slot=True),
            )

        await asyncio.wait_for(receiver.sem_prefetch.acquire(), timeout=0.1)

    assert exc_info.value is hook_error


async def test_callback_cancelled_before_start_releases_execution_capacity() -> None:
    broker = ListenerBroker(empty_listener)

    @broker.task(task_name="receiver.cancel-before-start")
    async def callback_task() -> None:
        return None

    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = Receiver(
            broker,
            executor=executor,
            run_startup=False,
            max_async_tasks=1,
            max_prefetch=0,
        )
        await receiver.sem_prefetch.acquire()
        await receiver._start_callback(
            _PrefetchedMessage(
                data=encoded_message(broker, callback_task.task_name),
                owns_prefetch_slot=True,
            ),
        )
        owned_task = next(iter(receiver._active_tasks))

        owned_task.cancel()
        await asyncio.gather(owned_task, return_exceptions=True)
        await asyncio.sleep(0)

        assert not receiver._active_tasks
        assert receiver.sem is not None
        await asyncio.wait_for(receiver.sem.acquire(), timeout=0.1)


async def test_unexpected_owned_callback_failure_is_retrieved_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    callback_error = ReceiverLifecycleError("owned callback failed")

    class FailingCallbackReceiver(Receiver):
        async def _run_owned_callback(
            self,
            message: bytes | AckableMessage,
        ) -> None:
            raise callback_error

    broker = ListenerBroker(empty_listener)
    caplog.set_level(logging.ERROR, logger="taskiq.receiver.receiver")
    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = FailingCallbackReceiver(
            broker,
            executor=executor,
            run_startup=False,
            max_async_tasks=1,
            max_prefetch=0,
        )
        await receiver.sem_prefetch.acquire()
        await receiver._start_callback(
            _PrefetchedMessage(data=b"message", owns_prefetch_slot=True),
        )
        owned_task = next(iter(receiver._active_tasks))

        await asyncio.gather(owned_task, return_exceptions=True)
        await asyncio.sleep(0)

    assert not receiver._active_tasks
    assert "Receiver callback failed outside task execution handling" in caplog.text


async def test_callback_cleanup_survives_outer_cancellation() -> None:
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def callback() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await release_cleanup.wait()

    callback_task = asyncio.create_task(callback())
    await asyncio.sleep(0)
    cleanup_task = asyncio.create_task(
        Receiver._cancel_callback_tasks({callback_task}),
    )
    await cleanup_started.wait()

    cleanup_task.cancel()
    await asyncio.sleep(0)
    release_cleanup.set()
    cancellation = await cleanup_task

    assert isinstance(cancellation, asyncio.CancelledError)
    assert callback_task.done()


async def test_active_callbacks_can_be_cancelled_without_graceful_wait() -> None:
    callback_started = asyncio.Event()
    callback_cleaned = asyncio.Event()

    async def callback() -> None:
        callback_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            callback_cleaned.set()

    broker = ListenerBroker(empty_listener)
    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = Receiver(broker, executor=executor, run_startup=False)
        callback_task = asyncio.create_task(callback())
        receiver._active_tasks.add(callback_task)
        await callback_started.wait()

        cancellation = await receiver._drain_active_tasks(cancel_immediately=True)

    assert cancellation is None
    assert callback_cleaned.is_set()
    assert callback_task.cancelled()


async def test_graceful_drain_preserves_outer_cancellation() -> None:
    callback_started = asyncio.Event()
    callback_cleaned = asyncio.Event()

    async def callback() -> None:
        callback_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            callback_cleaned.set()

    broker = ListenerBroker(empty_listener)
    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = Receiver(
            broker,
            executor=executor,
            run_startup=False,
            wait_tasks_timeout=None,
        )
        callback_task = asyncio.create_task(callback())
        receiver._active_tasks.add(callback_task)
        await callback_started.wait()
        drain_task = asyncio.create_task(
            receiver._drain_active_tasks(cancel_immediately=False),
        )
        await asyncio.sleep(0)
        assert not drain_task.done()

        drain_task.cancel()
        cancellation = await drain_task

    assert isinstance(cancellation, asyncio.CancelledError)
    assert callback_cleaned.is_set()
    assert callback_task.cancelled()


async def test_additional_listener_close_failure_keeps_primary_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    broker = ListenerBroker(empty_listener)
    first_error = ReceiverLifecycleError("primary listener failure")
    second_error = ReceiverLifecycleError("additional close failure")
    caplog.set_level(logging.ERROR, logger="taskiq.receiver.receiver")
    with ThreadPoolExecutor(max_workers=1) as executor:
        receiver = Receiver(broker, executor=executor, run_startup=False)

        receiver._record_listen_error(first_error)
        receiver._record_listen_error(second_error)

    assert receiver._listen_error is first_error
    assert "Additional error while closing broker listener" in caplog.text
