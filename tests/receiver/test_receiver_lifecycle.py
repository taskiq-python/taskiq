import asyncio
import logging
import multiprocessing
import os
from collections.abc import AsyncGenerator, Awaitable
from concurrent.futures import ProcessPoolExecutor
from io import StringIO
from pathlib import Path
from typing import Literal
from unittest.mock import Mock

import anyio
import pytest
from taskiq_dependencies import Depends

from taskiq.brokers.inmemory_broker import InmemoryResultBackend
from taskiq.exceptions import SendTaskError
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.middlewares import SimpleRetryMiddleware
from taskiq.receiver.receiver import Receiver, _PrefetchedMessage, _QueueSignal
from tests.receiver.receiver_lifecycle_support import (
    ControlledReceiver,
    DependencyCleanupProbe,
    ProcessResourceProbe,
    ReceiverQueue,
    ShieldCallCounter,
    SyncResourceProbe,
    assert_exact_capacity,
    listen_in_scope,
    process_resource_broker,
    read_process_resource,
    start_callback,
    wait_for_signals,
)
from tests.receiver.receiver_listener_support import (
    ReceiverLifecycleError,
    assert_semaphore_capacity,
)
from tests.utils import AsyncQueueBroker

RUNNER_PROBE_TASK_NAME = "receiver-drain-cancellation-probe"


@pytest.mark.parametrize("stop", ["timeout", "cancel", "scope"])
async def test_shutdown_waits_for_process_resource_owner(
    stop: Literal["timeout", "cancel", "scope"],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = multiprocessing.get_context("spawn")
    with (
        context.Manager() as manager,
        ProcessPoolExecutor(max_workers=1, mp_context=context) as executor,
    ):
        probe = ProcessResourceProbe(
            resource=tmp_path / "task-resource.txt",
            started=manager.Event(),
            release=manager.Event(),
        )
        probe.resource.write_text("completed")
        backend = InmemoryResultBackend[tuple[int, str]]()
        broker = process_resource_broker
        monkeypatch.setattr(broker, "queue", asyncio.Queue())
        monkeypatch.setattr(broker, "result_backend", backend)
        monkeypatch.setitem(broker.state, "process_resource", probe)
        task = await read_process_resource.kiq()
        receiver = Receiver(
            broker,
            executor=executor,
            max_async_tasks=1,
            run_startup=False,
            wait_tasks_timeout=0.01,
        )
        finish_event = asyncio.Event()
        scope = anyio.CancelScope()
        listener = asyncio.create_task(listen_in_scope(receiver, finish_event, scope))
        try:
            assert await asyncio.to_thread(probe.started.wait, 15)
            if stop == "timeout":
                finish_event.set()
            elif stop == "cancel":
                listener.cancel()
            else:
                scope.cancel()
            done, _ = await asyncio.wait({listener}, timeout=0.05)
            assert not done
            assert probe.resource.exists()
            assert not await backend.is_result_ready(task.task_id)
            probe.release.set()
            if stop == "cancel":
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(listener, timeout=5)
            else:
                await asyncio.wait_for(listener, timeout=5)
            result = await backend.get_result(task.task_id)
            assert not result.is_err
            child_pid, value = result.return_value
            assert child_pid != os.getpid()
            assert value == "completed"
            assert not probe.resource.exists()
            await asyncio.wait_for(broker.wait_tasks(), timeout=1)
            await assert_exact_capacity(receiver)
        finally:
            probe.release.set()
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)


@pytest.mark.parametrize("stop", ["timeout", "cancel", "scope"])
@pytest.mark.parametrize("returns_awaitable", [False, True])
async def test_shutdown_waits_for_sync_resource_owner(
    stop: str,
    returns_awaitable: bool,
) -> None:
    backend = InmemoryResultBackend[str]()
    broker = AsyncQueueBroker().with_result_backend(backend)
    probe = SyncResourceProbe()

    @broker.task
    def target(resource: StringIO = Depends(probe.dependency)) -> str | Awaitable[str]:
        value = probe.run(resource)
        return probe.continuation() if returns_awaitable else value

    task = await target.kiq()
    receiver = Receiver(
        broker,
        max_async_tasks=1,
        run_startup=False,
        wait_tasks_timeout=0.01,
    )
    finish_event = asyncio.Event()
    scope = anyio.CancelScope()
    listener = asyncio.create_task(listen_in_scope(receiver, finish_event, scope))
    try:
        await asyncio.wait_for(probe.started.wait(), timeout=1)
        if stop == "timeout":
            finish_event.set()
        elif stop == "cancel":
            listener.cancel()
        else:
            scope.cancel()
        done, _ = await asyncio.wait({listener}, timeout=0.05)
        assert not done
        assert not probe.resource.closed
        assert not await backend.is_result_ready(task.task_id)
        probe.release.set()
        if stop == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(listener, timeout=1)
        else:
            await asyncio.wait_for(listener, timeout=1)
        assert probe.finished.is_set()
        assert probe.resource.closed
        result = await backend.get_result(task.task_id)
        if returns_awaitable:
            assert probe.continuation_started.is_set()
            assert probe.continuation_finished.is_set()
            assert isinstance(result.error, asyncio.CancelledError)
        else:
            assert not result.is_err
            assert result.return_value == "completed"
        await asyncio.wait_for(broker.wait_tasks(), timeout=1)
        await assert_exact_capacity(receiver)
    finally:
        probe.release.set()
        probe.release_continuation.set()
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)
        await asyncio.wait_for(probe.finished.wait(), timeout=1)


@pytest.mark.parametrize("depth", [1, 2, 3])
async def test_shutdown_cancels_every_running_nested_call(depth: int) -> None:
    backend = InmemoryResultBackend[None]()
    broker = AsyncQueueBroker().with_result_backend(backend)
    started, release = asyncio.Event(), asyncio.Event()
    resumed: list[int] = []

    @broker.task
    async def target(level: int) -> None:
        if level:
            await receiver.run_task(
                target.original_func,
                TaskiqMessage(
                    task_id=f"nested-{level}",
                    task_name=target.task_name,
                    args=[level - 1],
                    kwargs={},
                    labels={},
                ),
            )
            resumed.append(level)
        else:
            started.set()
        await release.wait()

    task = await target.kiq(depth)
    receiver = Receiver(
        broker,
        max_async_tasks=1,
        run_startup=False,
        wait_tasks_timeout=0.01,
    )
    finish_event = asyncio.Event()
    listener = asyncio.create_task(receiver.listen(finish_event))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        finish_event.set()
        done, _ = await asyncio.wait({listener}, timeout=1)
        assert listener in done
        await listener
        assert not resumed
        result = await backend.get_result(task.task_id)
        assert isinstance(result.error, asyncio.CancelledError)
        await asyncio.wait_for(broker.wait_tasks(), timeout=1)
        await assert_exact_capacity(receiver)
    finally:
        release.set()
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)


@pytest.mark.parametrize("stop", ["timeout", "cancel", "scope"])
async def test_shutdown_preserves_nested_call_inside_target_cleanup(stop: str) -> None:
    backend = InmemoryResultBackend[None]()
    broker = AsyncQueueBroker().with_result_backend(backend)
    started, release = asyncio.Event(), asyncio.Event()
    cleanup_resumed, cleanup_finished = asyncio.Event(), asyncio.Event()

    @broker.task
    async def cleanup_step() -> None:
        await asyncio.sleep(0)

    @broker.task
    async def target() -> None:
        started.set()
        try:
            await release.wait()
        finally:
            await receiver.run_task(
                cleanup_step.original_func,
                TaskiqMessage(
                    task_id="target-cleanup",
                    task_name=cleanup_step.task_name,
                    args=[],
                    kwargs={},
                    labels={},
                ),
            )
            cleanup_resumed.set()
            await release.wait()
            cleanup_finished.set()

    task = await target.kiq()
    receiver = Receiver(
        broker,
        max_async_tasks=1,
        run_startup=False,
        wait_tasks_timeout=0.01,
    )
    finish_event = asyncio.Event()
    scope = anyio.CancelScope()
    listener = asyncio.create_task(listen_in_scope(receiver, finish_event, scope))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        if stop == "timeout":
            finish_event.set()
        elif stop == "cancel":
            listener.cancel()
        else:
            scope.cancel()
        await asyncio.wait_for(cleanup_resumed.wait(), timeout=1)
        assert not listener.done()
        assert not cleanup_finished.is_set()
        release.set()
        if stop == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(listener, timeout=1)
        else:
            await asyncio.wait_for(listener, timeout=1)
        assert cleanup_finished.is_set()
        result = await backend.get_result(task.task_id)
        assert isinstance(result.error, asyncio.CancelledError)
        await asyncio.wait_for(broker.wait_tasks(), timeout=1)
        await assert_exact_capacity(receiver)
    finally:
        release.set()
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)


@pytest.mark.parametrize("stop", ["timeout", "cancel", "scope"])
@pytest.mark.parametrize("error_type", [TimeoutError, ReceiverLifecycleError])
async def test_shutdown_survives_replaced_nested_cancellation(
    stop: str,
    error_type: type[Exception],
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = InmemoryResultBackend[None]()
    broker = AsyncQueueBroker().with_result_backend(backend)
    started, release, resumed = asyncio.Event(), asyncio.Event(), asyncio.Event()

    @broker.task
    async def child() -> None:
        started.set()
        try:
            await release.wait()
        finally:
            if error_type is TimeoutError:
                await release.wait()
            else:
                raise ReceiverLifecycleError("target cleanup failed")

    @broker.task
    async def parent() -> None:
        await receiver.run_task(
            child.original_func,
            TaskiqMessage(
                task_id="replaced-cancellation",
                task_name=child.task_name,
                args=[],
                kwargs={},
                labels={"timeout": 0.2} if error_type is TimeoutError else {},
            ),
        )
        resumed.set()
        await release.wait()

    task = await parent.kiq()
    receiver = Receiver(
        broker,
        max_async_tasks=1,
        run_startup=False,
        wait_tasks_timeout=0.01,
    )
    finish_event = asyncio.Event()
    scope = anyio.CancelScope()
    listener = asyncio.create_task(listen_in_scope(receiver, finish_event, scope))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        if stop == "timeout":
            finish_event.set()
        elif stop == "cancel":
            listener.cancel()
        else:
            scope.cancel()
        done, _ = await asyncio.wait({listener}, timeout=1)
        assert listener in done
        if stop == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await listener
        else:
            await listener
        assert not resumed.is_set()
        result = await backend.get_result(task.task_id)
        assert isinstance(result.error, asyncio.CancelledError)
        assert any(
            record.exc_info and isinstance(record.exc_info[1], error_type)
            for record in caplog.records
        )
        await asyncio.wait_for(broker.wait_tasks(), timeout=1)
        await assert_exact_capacity(receiver)
    finally:
        release.set()
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)


@pytest.mark.parametrize("stop", ["timeout", "cancel", "scope"])
@pytest.mark.parametrize("nested", [False, True])
async def test_shutdown_preserves_started_dependency_teardown(
    stop: str,
    nested: bool,
) -> None:
    backend = InmemoryResultBackend[str]()
    broker = AsyncQueueBroker().with_result_backend(backend)
    probe = DependencyCleanupProbe()
    resumed = asyncio.Event()

    @broker.task
    async def child(resource: None = Depends(probe.dependency)) -> str:
        return "completed"

    @broker.task
    async def parent() -> None:
        await receiver.run_task(
            child.original_func,
            TaskiqMessage(
                task_id="nested-cleanup",
                task_name=child.task_name,
                args=[],
                kwargs={},
                labels={},
            ),
        )
        resumed.set()
        await asyncio.Event().wait()

    task = await (parent if nested else child).kiq()
    receiver = Receiver(
        broker,
        max_async_tasks=1,
        run_startup=False,
        wait_tasks_timeout=0.01,
    )
    finish_event = asyncio.Event()
    scope = anyio.CancelScope()
    listener = asyncio.create_task(listen_in_scope(receiver, finish_event, scope))
    try:
        await asyncio.wait_for(probe.started.wait(), timeout=1)
        if stop == "timeout":
            finish_event.set()
        elif stop == "cancel":
            listener.cancel()
        else:
            scope.cancel()
        done, _ = await asyncio.wait({listener}, timeout=0.05)
        assert not done
        assert not probe.finished.is_set()
        if stop == "cancel":
            listener.cancel()
        probe.release.set()
        if stop == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(listener, timeout=1)
        else:
            await asyncio.wait_for(listener, timeout=1)
        assert probe.finished.is_set()
        assert not resumed.is_set()
        result = await backend.get_result(task.task_id)
        if nested:
            assert isinstance(result.error, asyncio.CancelledError)
        else:
            assert not result.is_err
            assert result.return_value == "completed"
        await asyncio.wait_for(broker.wait_tasks(), timeout=1)
        await assert_exact_capacity(receiver)
    finally:
        probe.release.set()
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)


async def test_shutdown_preserves_reentrant_dependency_teardown() -> None:
    backend = InmemoryResultBackend[str]()
    broker = AsyncQueueBroker().with_result_backend(backend)
    probe = DependencyCleanupProbe()
    outer_started, release_outer, outer_finished = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )

    @broker.task
    async def child(resource: None = Depends(probe.dependency)) -> None:
        pass

    async def outer_dependency() -> AsyncGenerator[None, None]:
        try:
            yield
        finally:
            await receiver.run_task(
                child.original_func,
                TaskiqMessage(
                    task_id="nested-close",
                    task_name=child.task_name,
                    args=[],
                    kwargs={},
                    labels={},
                ),
            )
            outer_started.set()
            await release_outer.wait()
            outer_finished.set()

    @broker.task
    async def parent(resource: None = Depends(outer_dependency)) -> str:
        return "completed"

    task = await parent.kiq()
    receiver = Receiver(broker, run_startup=False, wait_tasks_timeout=0.01)
    finish_event = asyncio.Event()
    listener = asyncio.create_task(receiver.listen(finish_event))
    try:
        await asyncio.wait_for(probe.started.wait(), timeout=1)
        finish_event.set()
        await asyncio.wait({listener}, timeout=0.05)
        probe.release.set()
        await asyncio.wait_for(outer_started.wait(), timeout=1)
        assert probe.finished.is_set()
        assert not listener.done()
        release_outer.set()
        await asyncio.wait_for(listener, timeout=1)
        assert outer_finished.is_set()
        assert not (await backend.get_result(task.task_id)).is_err
        await asyncio.wait_for(broker.wait_tasks(), timeout=1)
    finally:
        probe.release.set()
        release_outer.set()
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)


async def test_shutdown_finishes_nested_retry_before_cancelling_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = AsyncQueueBroker().with_middlewares(SimpleRetryMiddleware())
    probe = DependencyCleanupProbe()
    send_started, release_send = asyncio.Event(), asyncio.Event()
    original_kick = broker.kick

    async def blocked_kick(message: BrokerMessage) -> None:
        send_started.set()
        await release_send.wait()
        await original_kick(message)

    @broker.task(retry_on_error=True)
    async def child(resource: None = Depends(probe.dependency)) -> None:
        raise ValueError("retry this task")

    @broker.task
    async def parent() -> None:
        await receiver.run_task(
            child.original_func,
            TaskiqMessage(
                task_id="nested-retry",
                task_name=child.task_name,
                args=[],
                kwargs={},
                labels={"retry_on_error": True},
            ),
        )

    await parent.kiq()
    monkeypatch.setattr(broker, "kick", blocked_kick)
    receiver = Receiver(broker, run_startup=False, wait_tasks_timeout=0.01)
    finish_event = asyncio.Event()
    listener = asyncio.create_task(receiver.listen(finish_event))
    try:
        await asyncio.wait_for(probe.started.wait(), timeout=1)
        finish_event.set()
        await asyncio.wait({listener}, timeout=0.05)
        probe.release.set()
        await asyncio.wait_for(send_started.wait(), timeout=1)
        assert not listener.done()
        release_send.set()
        await asyncio.wait_for(listener, timeout=1)
        retry = broker.formatter.loads(await asyncio.wait_for(broker.queue.get(), 1))
        retry.parse_labels()
        assert retry.task_name == child.task_name
        assert retry.task_id == "nested-retry"
        assert retry.labels["_retries"] == 1
        broker.queue.task_done()
        await asyncio.wait_for(broker.wait_tasks(), timeout=1)
    finally:
        probe.release.set()
        release_send.set()
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)


async def test_shutdown_delivers_cancellation_after_retry_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = AsyncQueueBroker().with_middlewares(SimpleRetryMiddleware())
    send_started, release_send, release_parent = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )
    failures: list[SendTaskError] = []

    async def failing_kick(message: BrokerMessage) -> None:
        send_started.set()
        await release_send.wait()
        raise ReceiverLifecycleError("retry send failed")

    @broker.task(retry_on_error=True)
    async def child() -> None:
        raise ValueError("retry this task")

    @broker.task
    async def parent() -> None:
        try:
            await receiver.run_task(
                child.original_func,
                TaskiqMessage(
                    task_id="nested-retry-failure",
                    task_name=child.task_name,
                    args=[],
                    kwargs={},
                    labels={"retry_on_error": True},
                ),
            )
        except SendTaskError as error:
            failures.append(error)
            await release_parent.wait()

    await parent.kiq()
    monkeypatch.setattr(broker, "kick", failing_kick)
    receiver = Receiver(broker, run_startup=False, wait_tasks_timeout=0.01)
    finish_event = asyncio.Event()
    listener = asyncio.create_task(receiver.listen(finish_event))
    try:
        await asyncio.wait_for(send_started.wait(), timeout=1)
        finish_event.set()
        done, _ = await asyncio.wait({listener}, timeout=0.05)
        assert not done
        release_send.set()
        done, _ = await asyncio.wait({listener}, timeout=1)
        assert listener in done
        await listener
        assert len(failures) == 1
        assert isinstance(failures[0].__cause__, ReceiverLifecycleError)
        await asyncio.wait_for(broker.wait_tasks(), timeout=1)
    finally:
        release_send.set()
        release_parent.set()
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)


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
