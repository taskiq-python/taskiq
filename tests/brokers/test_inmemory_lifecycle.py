import asyncio
import logging
from typing import Any

import pytest

from taskiq import InMemoryBroker
from taskiq.events import TaskiqEvents
from taskiq.state import TaskiqState
from tests.brokers.inmemory_contract_support import (
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


async def test_shutdown_cancellation_still_closes_every_resource() -> None:
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
        await shutdown_task

    assert events == [
        "middleware.startup",
        "backend.startup",
        "client.shutdown",
        "middleware.shutdown",
        "backend.shutdown",
    ]
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
