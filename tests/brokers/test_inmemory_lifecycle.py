import asyncio
import logging
import threading
from typing import Any

import anyio
import pytest

from taskiq import InMemoryBroker, TaskiqMessage
from taskiq.events import TaskiqEvents
from taskiq.exceptions import SendTaskError
from taskiq.state import TaskiqState
from tests.brokers.inmemory_contract_support import (
    BlockingPreSendMiddleware,
    BlockingShutdownMiddleware,
    BlockingShutdownResultBackend,
    DrainSignallingInMemoryBroker,
    FailingExecutionMiddleware,
    LifecycleError,
    RecordingLifecycleMiddleware,
    RecordingResultBackend,
)


async def test_lifecycle_runs_events_and_resources_once_in_order() -> None:
    events: list[str] = []
    broker = InMemoryBroker()
    broker.with_middlewares(RecordingLifecycleMiddleware(events))
    broker.with_result_backend(RecordingResultBackend(events))

    @broker.on_event(TaskiqEvents.CLIENT_STARTUP)
    def record_client_startup(state: TaskiqState) -> None:
        events.append("client.startup")

    @broker.on_event(TaskiqEvents.WORKER_STARTUP)
    def record_worker_startup(state: TaskiqState) -> None:
        events.append("worker.startup")

    @broker.on_event(TaskiqEvents.CLIENT_SHUTDOWN)
    def record_client_shutdown(state: TaskiqState) -> None:
        events.append("client.shutdown")

    @broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
    def record_worker_shutdown(state: TaskiqState) -> None:
        events.append("worker.shutdown")

    await broker.startup()
    await broker.shutdown()
    await broker.shutdown()

    assert events == [
        "client.startup",
        "worker.startup",
        "middleware.startup",
        "backend.startup",
        "client.shutdown",
        "worker.shutdown",
        "middleware.shutdown",
        "backend.shutdown",
    ]


async def test_shutdown_cleans_resources_after_startup_failure() -> None:
    events: list[str] = []
    startup_error = LifecycleError("backend startup failed")
    broker = InMemoryBroker()
    broker.with_middlewares(RecordingLifecycleMiddleware(events))
    broker.with_result_backend(RecordingResultBackend(events, startup_error))

    with pytest.raises(LifecycleError) as exc_info:
        await broker.startup()
    assert exc_info.value is startup_error

    await broker.shutdown()

    assert events == [
        "middleware.startup",
        "backend.startup",
        "middleware.shutdown",
        "backend.shutdown",
    ]


async def test_shutdown_keeps_first_failure_and_closes_every_resource(
    caplog: pytest.LogCaptureFixture,
) -> None:
    events: list[str] = []
    event_error = LifecycleError("event shutdown failed")
    middleware_error = LifecycleError("middleware shutdown failed")
    backend_error = LifecycleError("backend shutdown failed")
    broker = InMemoryBroker()
    broker.with_middlewares(
        RecordingLifecycleMiddleware(events, shutdown_error=middleware_error),
    )
    broker.with_result_backend(
        RecordingResultBackend(events, shutdown_error=backend_error),
    )

    @broker.on_event(TaskiqEvents.CLIENT_SHUTDOWN)
    def fail_client_shutdown(state: TaskiqState) -> None:
        events.append("client.shutdown")
        raise event_error

    @broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
    def record_worker_shutdown(state: TaskiqState) -> None:
        events.append("worker.shutdown")

    caplog.set_level(logging.ERROR, logger="taskiq")
    await broker.startup()

    with pytest.raises(LifecycleError) as exc_info:
        await broker.shutdown()

    assert exc_info.value is event_error
    assert events == [
        "middleware.startup",
        "backend.startup",
        "client.shutdown",
        "worker.shutdown",
        "middleware.shutdown",
        "backend.shutdown",
    ]
    assert caplog.text.count("Additional error while shutting down") == 2
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)

    await broker.shutdown()
    assert events == [
        "middleware.startup",
        "backend.startup",
        "client.shutdown",
        "worker.shutdown",
        "middleware.shutdown",
        "backend.shutdown",
    ]


async def test_shutdown_cancellation_during_event_is_retryable() -> None:
    events: list[str] = []
    shutdown_started = asyncio.Event()
    keep_shutdown_blocked = asyncio.Event()
    broker = InMemoryBroker()
    broker.with_middlewares(RecordingLifecycleMiddleware(events))
    broker.with_result_backend(RecordingResultBackend(events))

    @broker.on_event(TaskiqEvents.CLIENT_SHUTDOWN)
    async def block_client_shutdown(state: TaskiqState) -> None:
        events.append("client.shutdown")
        shutdown_started.set()
        await keep_shutdown_blocked.wait()

    await broker.startup()
    shutdown_task = asyncio.create_task(broker.shutdown())
    await asyncio.wait_for(shutdown_started.wait(), timeout=1)
    shutdown_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(shutdown_task, timeout=1)

    assert events == [
        "middleware.startup",
        "backend.startup",
        "client.shutdown",
    ]
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)

    keep_shutdown_blocked.set()
    await broker.shutdown()

    assert events == [
        "middleware.startup",
        "backend.startup",
        "client.shutdown",
        "client.shutdown",
        "middleware.shutdown",
        "backend.shutdown",
    ]
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)


@pytest.mark.parametrize("resource_kind", ["middleware", "backend"])
async def test_shutdown_cancellation_during_resource_is_retryable(
    resource_kind: str,
) -> None:
    shutdown_started = asyncio.Event()
    release_shutdown = asyncio.Event()
    shutdown_finished = asyncio.Event()
    broker = InMemoryBroker()

    if resource_kind == "middleware":
        broker.with_middlewares(
            BlockingShutdownMiddleware(
                shutdown_started,
                release_shutdown,
                shutdown_finished,
            ),
        )
    else:
        broker.with_result_backend(
            BlockingShutdownResultBackend(
                shutdown_started,
                release_shutdown,
                shutdown_finished,
            ),
        )

    await broker.startup()
    shutdown_task = asyncio.create_task(broker.shutdown())
    await asyncio.wait_for(shutdown_started.wait(), timeout=1)
    shutdown_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(shutdown_task, timeout=1)

    assert not shutdown_finished.is_set()
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)

    release_shutdown.set()
    await broker.shutdown()

    assert shutdown_finished.is_set()
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)


async def test_shutdown_preserves_failure_that_precedes_cancellation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    shutdown_started = asyncio.Event()
    release_shutdown = asyncio.Event()
    shutdown_finished = asyncio.Event()
    lifecycle_error = LifecycleError("shutdown handler failed")
    shutdown_event_calls = 0
    broker = InMemoryBroker().with_middlewares(
        BlockingShutdownMiddleware(
            shutdown_started,
            release_shutdown,
            shutdown_finished,
        ),
    )

    @broker.on_event(TaskiqEvents.CLIENT_SHUTDOWN)
    def fail_shutdown(state: TaskiqState) -> None:
        nonlocal shutdown_event_calls
        shutdown_event_calls += 1
        raise lifecycle_error

    caplog.set_level(logging.ERROR, logger="taskiq")
    shutdown_task = asyncio.create_task(broker.shutdown())
    await asyncio.wait_for(shutdown_started.wait(), timeout=1)
    shutdown_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(shutdown_task, timeout=1)

    assert not shutdown_finished.is_set()
    assert caplog.text.count("Additional error while shutting down") == 0
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)

    release_shutdown.set()
    with pytest.raises(LifecycleError) as retry_exc_info:
        await broker.shutdown()

    assert retry_exc_info.value is lifecycle_error
    assert shutdown_finished.is_set()
    assert shutdown_event_calls == 1
    assert caplog.text.count("Additional error while shutting down") == 0
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)


async def test_concurrent_shutdown_closes_resources_once() -> None:
    shutdown_started = asyncio.Event()
    release_shutdown = asyncio.Event()
    shutdown_finished = asyncio.Event()
    middleware = BlockingShutdownMiddleware(
        shutdown_started,
        release_shutdown,
        shutdown_finished,
    )
    broker = InMemoryBroker().with_middlewares(middleware)

    first_shutdown = asyncio.create_task(broker.shutdown())
    await asyncio.wait_for(shutdown_started.wait(), timeout=1)
    second_shutdown = asyncio.create_task(broker.shutdown())
    await asyncio.sleep(0)

    assert not second_shutdown.done()
    release_shutdown.set()
    await asyncio.wait_for(
        asyncio.gather(first_shutdown, second_shutdown),
        timeout=1,
    )

    assert middleware.shutdown_calls == 1
    assert shutdown_finished.is_set()
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)


async def test_shutdown_retry_skips_completed_resources() -> None:
    events: list[str] = []
    shutdown_started = asyncio.Event()
    release_shutdown = asyncio.Event()
    shutdown_finished = asyncio.Event()
    completed_middleware = RecordingLifecycleMiddleware(events)
    blocking_middleware = BlockingShutdownMiddleware(
        shutdown_started,
        release_shutdown,
        shutdown_finished,
    )
    broker = InMemoryBroker().with_middlewares(
        completed_middleware,
        blocking_middleware,
    )

    first_attempt = asyncio.create_task(broker.shutdown())
    await asyncio.wait_for(shutdown_started.wait(), timeout=1)
    first_attempt.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(first_attempt, timeout=1)

    assert events == ["middleware.shutdown"]
    assert blocking_middleware.shutdown_calls == 1

    release_shutdown.set()
    await broker.shutdown()

    assert events == ["middleware.shutdown"]
    assert blocking_middleware.shutdown_calls == 2
    assert shutdown_finished.is_set()


async def test_task_failure_does_not_shield_resource_cancellation() -> None:
    shutdown_started = asyncio.Event()
    release_shutdown = asyncio.Event()
    shutdown_finished = asyncio.Event()
    shutdown_exited = asyncio.Event()
    task_error = LifecycleError("in-memory task failed")
    broker = InMemoryBroker()
    broker.with_middlewares(FailingExecutionMiddleware(task_error))
    broker.with_middlewares(
        BlockingShutdownMiddleware(
            shutdown_started,
            release_shutdown,
            shutdown_finished,
        ),
    )

    @broker.task
    async def failing_task() -> None:
        return None

    await failing_task.kiq()
    running_task = next(iter(broker._running_tasks))
    await asyncio.wait((running_task,))
    assert not broker._running_tasks

    cancel_scope = anyio.CancelScope()

    async def run_shutdown() -> None:
        with cancel_scope:
            await broker.shutdown()
        shutdown_exited.set()

    shutdown_task = asyncio.create_task(run_shutdown())
    await asyncio.wait_for(shutdown_started.wait(), timeout=1)
    cancel_scope.cancel()

    try:
        await asyncio.wait_for(shutdown_exited.wait(), timeout=1)
    finally:
        release_shutdown.set()
        await asyncio.wait_for(shutdown_task, timeout=1)

    assert not shutdown_finished.is_set()
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)

    with pytest.raises(LifecycleError) as exc_info:
        await broker.shutdown()

    assert exc_info.value is task_error
    assert shutdown_finished.is_set()
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)


async def test_cancelled_shutdown_drains_before_resource_cleanup() -> None:
    task_started = asyncio.Event()
    finish_task = asyncio.Event()
    drain_started = asyncio.Event()
    drain_restarted = asyncio.Event()
    events: list[str] = []
    broker = DrainSignallingInMemoryBroker(drain_started, drain_restarted)
    broker.with_middlewares(RecordingLifecycleMiddleware(events))
    broker.with_result_backend(RecordingResultBackend(events))

    @broker.task
    async def running_task() -> None:
        events.append("task.started")
        task_started.set()
        await finish_task.wait()
        events.append("task.finished")

    await broker.startup()
    await running_task.kiq()
    await asyncio.wait_for(task_started.wait(), timeout=1)

    shutdown_task = asyncio.create_task(broker.shutdown())
    await asyncio.wait_for(drain_started.wait(), timeout=1)
    shutdown_task.cancel()
    await asyncio.wait_for(drain_restarted.wait(), timeout=1)

    assert not shutdown_task.done()
    finish_task.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(shutdown_task), timeout=1)

    assert events == [
        "middleware.startup",
        "backend.startup",
        "task.started",
        "task.finished",
        "middleware.shutdown",
        "backend.shutdown",
    ]
    assert not broker._running_tasks
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)


async def test_anyio_cancellation_does_not_spin_while_draining() -> None:
    task_started = asyncio.Event()
    finish_task = asyncio.Event()
    drain_started = asyncio.Event()
    broker = DrainSignallingInMemoryBroker(drain_started)

    @broker.task
    async def running_task() -> None:
        task_started.set()
        await finish_task.wait()

    await running_task.kiq()
    await asyncio.wait_for(task_started.wait(), timeout=1)

    cancel_scope = anyio.CancelScope()

    async def cancel_then_release() -> None:
        await drain_started.wait()
        cancel_scope.cancel()
        await asyncio.sleep(0)
        finish_task.set()

    cancellation_driver = asyncio.create_task(cancel_then_release())
    with cancel_scope:
        await broker.shutdown()
    await asyncio.wait_for(cancellation_driver, timeout=1)

    assert broker.drain_calls == 2
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)


async def test_shutdown_waits_for_running_tasks_before_resource_cleanup() -> None:
    task_started = asyncio.Event()
    finish_task = asyncio.Event()
    drain_started = asyncio.Event()
    events: list[str] = []
    broker = DrainSignallingInMemoryBroker(drain_started)
    broker.with_middlewares(RecordingLifecycleMiddleware(events))
    broker.with_result_backend(RecordingResultBackend(events))

    @broker.on_event(TaskiqEvents.CLIENT_SHUTDOWN)
    def record_client_shutdown(state: TaskiqState) -> None:
        events.append("client.shutdown")

    @broker.task
    async def running_task() -> None:
        events.append("task.started")
        task_started.set()
        await finish_task.wait()
        events.append("task.finished")

    await broker.startup()
    await running_task.kiq()
    await asyncio.wait_for(task_started.wait(), timeout=1)

    shutdown_task = asyncio.create_task(broker.shutdown())
    await asyncio.wait_for(drain_started.wait(), timeout=1)
    assert not shutdown_task.done()

    finish_task.set()
    await asyncio.wait_for(shutdown_task, timeout=1)

    assert events == [
        "middleware.startup",
        "backend.startup",
        "task.started",
        "task.finished",
        "client.shutdown",
        "middleware.shutdown",
        "backend.shutdown",
    ]
    assert not broker._running_tasks


async def test_shutdown_waits_for_cancelled_inline_sync_execution() -> None:
    events: list[str] = []
    task_started = threading.Event()
    finish_task = threading.Event()
    broker = InMemoryBroker(await_inplace=True)
    broker.with_middlewares(RecordingLifecycleMiddleware(events))

    @broker.task
    def sync_task() -> None:
        events.append("task.started")
        task_started.set()
        finish_task.wait()
        events.append("task.finished")

    await broker.startup()
    sender = asyncio.create_task(sync_task.kiq())
    assert await asyncio.to_thread(task_started.wait, 1)
    sender.cancel()
    await sender

    release_timer = threading.Timer(0.05, finish_task.set)
    release_timer.start()
    try:
        await broker.shutdown()
    finally:
        release_timer.cancel()

    assert events == [
        "middleware.startup",
        "task.started",
        "task.finished",
        "middleware.shutdown",
    ]


async def test_shutdown_keeps_loop_responsive_for_cancelled_sync_execution() -> None:
    loop = asyncio.get_running_loop()
    task_started = threading.Event()
    finish_task = threading.Event()
    loop_callback_finished = asyncio.Event()
    broker = InMemoryBroker(await_inplace=True)

    async def finish_on_loop() -> None:
        loop_callback_finished.set()

    @broker.task
    def sync_task() -> None:
        task_started.set()
        finish_task.wait()
        callback = asyncio.run_coroutine_threadsafe(finish_on_loop(), loop)
        callback.result(timeout=1)

    sender = asyncio.create_task(sync_task.kiq())
    assert await asyncio.to_thread(task_started.wait, 1)
    sender.cancel()
    await sender

    finish_task.set()
    await asyncio.wait_for(broker.shutdown(), timeout=1)

    assert loop_callback_finished.is_set()


async def test_shutdown_defers_cancellation_while_closing_sync_executor() -> None:
    events: list[str] = []
    task_started = threading.Event()
    finish_task = threading.Event()
    broker = InMemoryBroker(await_inplace=True)
    broker.with_middlewares(RecordingLifecycleMiddleware(events))

    @broker.task
    def sync_task() -> None:
        events.append("task.started")
        task_started.set()
        finish_task.wait()
        events.append("task.finished")

    await broker.startup()
    sender = asyncio.create_task(sync_task.kiq())
    assert await asyncio.to_thread(task_started.wait, 1)
    sender.cancel()
    await sender

    shutdown_task = asyncio.create_task(broker.shutdown())
    await asyncio.sleep(0)
    assert not shutdown_task.done()
    shutdown_task.cancel()
    release_timer = threading.Timer(0.05, finish_task.set)
    release_timer.start()
    try:
        with pytest.raises(asyncio.CancelledError):
            await shutdown_task
    finally:
        release_timer.cancel()

    assert events == [
        "middleware.startup",
        "task.started",
        "task.finished",
        "middleware.shutdown",
    ]


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_shutdown_waits_for_send_blocked_in_pre_send(
    await_inplace: bool,
) -> None:
    pre_send_started = asyncio.Event()
    release_pre_send = asyncio.Event()
    resource_shutdown_started = asyncio.Event()
    drain_started = asyncio.Event()
    task_executed = asyncio.Event()
    middleware = BlockingPreSendMiddleware(
        pre_send_started,
        release_pre_send,
        resource_shutdown_started,
    )
    broker = DrainSignallingInMemoryBroker(
        drain_started,
        await_inplace=await_inplace,
    ).with_middlewares(middleware)

    @broker.task
    async def task() -> None:
        task_executed.set()

    sender = asyncio.create_task(task.kiq())
    await asyncio.wait_for(pre_send_started.wait(), timeout=1)
    shutdown_task = asyncio.create_task(broker.shutdown())
    await asyncio.wait_for(drain_started.wait(), timeout=1)

    assert not shutdown_task.done()
    assert not resource_shutdown_started.is_set()

    release_pre_send.set()
    await asyncio.wait_for(sender, timeout=1)
    await asyncio.wait_for(shutdown_task, timeout=1)

    assert task_executed.is_set()
    assert resource_shutdown_started.is_set()
    assert middleware.pre_send_calls == 1
    assert not broker._inflight_tasks
    assert not broker._running_tasks


async def test_shutdown_rejects_send_before_pre_send_side_effects() -> None:
    pre_send_started = asyncio.Event()
    release_pre_send = asyncio.Event()
    release_pre_send.set()
    resource_shutdown_started = asyncio.Event()
    middleware = BlockingPreSendMiddleware(
        pre_send_started,
        release_pre_send,
        resource_shutdown_started,
    )
    broker = InMemoryBroker().with_middlewares(middleware)

    @broker.task
    async def task() -> None:
        return None

    await broker.shutdown()

    with pytest.raises(SendTaskError) as exc_info:
        await task.kiq()

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert middleware.pre_send_calls == 0
    assert not pre_send_started.is_set()


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_shutdown_rejects_work_after_drain_starts(
    await_inplace: bool,
) -> None:
    shutdown_started = asyncio.Event()
    finish_shutdown = asyncio.Event()
    task_executed = False
    broker = InMemoryBroker(await_inplace=await_inplace)

    @broker.on_event(TaskiqEvents.CLIENT_SHUTDOWN)
    async def block_client_shutdown(state: TaskiqState) -> None:
        shutdown_started.set()
        await finish_shutdown.wait()

    @broker.task
    async def task() -> None:
        nonlocal task_executed
        task_executed = True

    shutdown_task = asyncio.create_task(broker.shutdown())
    await asyncio.wait_for(shutdown_started.wait(), timeout=1)

    with pytest.raises(SendTaskError) as exc_info:
        await task.kiq()

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    message = TaskiqMessage(
        task_id="late-direct-task-id",
        task_name=task.task_name,
        labels={},
        labels_types={},
        args=[],
        kwargs={},
    )
    with pytest.raises(RuntimeError, match="shutting down"):
        await broker.kick(broker.formatter.dumps(message))

    assert not task_executed
    finish_shutdown.set()
    await asyncio.wait_for(shutdown_task, timeout=1)
    assert not broker._inflight_tasks
    assert not broker._running_tasks


async def test_wait_all_retains_failure_from_already_completed_task() -> None:
    task_error = LifecycleError("completed in-memory task failed")
    broker = InMemoryBroker()
    broker.with_middlewares(FailingExecutionMiddleware(task_error))

    @broker.task
    async def failing_task() -> None:
        return None

    await failing_task.kiq()
    running_task = next(iter(broker._running_tasks))
    task_completed = asyncio.Event()

    def mark_completed(completed_task: asyncio.Task[Any]) -> None:
        assert completed_task.done()
        task_completed.set()

    running_task.add_done_callback(mark_completed)
    await asyncio.wait_for(task_completed.wait(), timeout=1)
    assert not broker._running_tasks

    with pytest.raises(LifecycleError) as exc_info:
        await broker.wait_all()

    assert exc_info.value is task_error
    await broker.wait_all()
    await broker.shutdown()


async def test_shutdown_cleans_resources_after_task_failure() -> None:
    events: list[str] = []
    task_error = LifecycleError("in-memory task failed")
    middleware_error = LifecycleError("middleware shutdown failed")
    broker = InMemoryBroker()
    broker.with_middlewares(FailingExecutionMiddleware(task_error))
    broker.with_middlewares(
        RecordingLifecycleMiddleware(events, shutdown_error=middleware_error),
    )
    broker.with_result_backend(RecordingResultBackend(events))

    @broker.task
    async def failing_task() -> None:
        return None

    await broker.startup()
    await failing_task.kiq()

    with pytest.raises(LifecycleError) as exc_info:
        await broker.shutdown()

    assert exc_info.value is task_error
    assert events == [
        "middleware.startup",
        "backend.startup",
        "middleware.shutdown",
        "backend.shutdown",
    ]
    assert not broker._running_tasks
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)
