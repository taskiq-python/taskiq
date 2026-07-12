import asyncio
import logging
import uuid
from typing import Any

import pytest

from taskiq import Flow, InMemoryBroker, TaskiqMessage
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.brokers.inmemory_broker import InmemoryResultBackend
from taskiq.events import TaskiqEvents
from taskiq.state import TaskiqState


class LifecycleError(Exception):
    """Marker exception for lifecycle fault tests."""


class RecordingLifecycleMiddleware(TaskiqMiddleware):
    """Record middleware resource lifecycle calls."""

    def __init__(
        self,
        events: list[str],
        shutdown_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self.events = events
        self.shutdown_error = shutdown_error

    async def startup(self) -> None:
        """Record startup."""
        self.events.append("middleware.startup")

    async def shutdown(self) -> None:
        """Record shutdown."""
        self.events.append("middleware.shutdown")
        if self.shutdown_error is not None:
            raise self.shutdown_error


class RecordingResultBackend(InmemoryResultBackend[Any]):
    """Record result backend lifecycle calls and optional failures."""

    def __init__(
        self,
        events: list[str],
        startup_error: Exception | None = None,
        shutdown_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self.events = events
        self.startup_error = startup_error
        self.shutdown_error = shutdown_error

    async def startup(self) -> None:
        """Record startup and raise the configured error."""
        self.events.append("backend.startup")
        if self.startup_error is not None:
            raise self.startup_error

    async def shutdown(self) -> None:
        """Record shutdown."""
        self.events.append("backend.shutdown")
        if self.shutdown_error is not None:
            raise self.shutdown_error


class FailingExecutionMiddleware(TaskiqMiddleware):
    """Raise before execution to expose background callback failures."""

    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error

    def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        """Raise the configured callback failure."""
        raise self.error


async def test_inmemory_success() -> None:
    broker = InMemoryBroker()
    test_val = uuid.uuid4().hex

    @broker.task
    async def task() -> str:
        return test_val

    kicked = await task.kiq()
    result = await kicked.wait_result()
    assert result.return_value == test_val
    assert not broker._running_tasks


async def test_inmemory_accepts_explicit_flow_as_local_metadata() -> None:
    flow = Flow("local.priority")
    broker = InMemoryBroker(default_flow=flow, await_inplace=True)

    @broker.task
    async def task() -> str:
        return flow.name

    kicked = await task.kiq()
    result = await kicked.wait_result()

    assert result.return_value == "local.priority"


async def test_cannot_listen() -> None:
    broker = InMemoryBroker()

    with pytest.raises(RuntimeError):
        async for _ in broker.listen():
            pass


async def test_startup() -> None:
    broker = InMemoryBroker()
    test_value = uuid.uuid4().hex

    @broker.on_event(TaskiqEvents.WORKER_STARTUP)
    async def _w_startup(state: TaskiqState) -> None:
        state.from_worker = test_value

    @broker.on_event(TaskiqEvents.CLIENT_STARTUP)
    async def _c_startup(state: TaskiqState) -> None:
        state.from_client = test_value

    await broker.startup()

    assert broker.state.from_worker == test_value
    assert broker.state.from_client == test_value


async def test_lifecycle_runs_events_and_resources_once_in_order() -> None:
    events: list[str] = []
    broker = InMemoryBroker()
    broker.with_middlewares(RecordingLifecycleMiddleware(events))
    broker.with_result_backend(RecordingResultBackend(events))

    for event, event_name in (
        (TaskiqEvents.CLIENT_STARTUP, "client.startup"),
        (TaskiqEvents.WORKER_STARTUP, "worker.startup"),
        (TaskiqEvents.CLIENT_SHUTDOWN, "client.shutdown"),
        (TaskiqEvents.WORKER_SHUTDOWN, "worker.shutdown"),
    ):

        @broker.on_event(event)
        def record_event(
            state: TaskiqState,
            lifecycle_event: str = event_name,
        ) -> None:
            events.append(lifecycle_event)

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


async def test_shutdown_closes_remaining_resources_after_middleware_failure() -> None:
    events: list[str] = []
    shutdown_error = LifecycleError("middleware shutdown failed")
    broker = InMemoryBroker()
    broker.with_middlewares(
        RecordingLifecycleMiddleware(events, shutdown_error=shutdown_error),
    )
    broker.with_result_backend(RecordingResultBackend(events))

    await broker.startup()

    with pytest.raises(LifecycleError) as exc_info:
        await broker.shutdown()

    assert exc_info.value is shutdown_error
    assert events == [
        "middleware.startup",
        "backend.startup",
        "middleware.shutdown",
        "backend.shutdown",
    ]
    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        broker.executor.submit(int)


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


async def test_shutdown() -> None:
    broker = InMemoryBroker()
    test_value = uuid.uuid4().hex

    @broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
    async def _w_startup(state: TaskiqState) -> None:
        state.from_worker = test_value

    @broker.on_event(TaskiqEvents.CLIENT_SHUTDOWN)
    async def _c_startup(state: TaskiqState) -> None:
        state.from_client = test_value

    await broker.shutdown()

    assert broker.state.from_worker == test_value
    assert broker.state.from_client == test_value


async def test_execution() -> None:
    broker = InMemoryBroker()
    test_value = uuid.uuid4().hex

    @broker.task
    async def test_task() -> str:
        await asyncio.sleep(0.5)
        return test_value

    task = await test_task.kiq()
    assert not await task.is_ready()

    result = await task.wait_result()
    assert result.return_value == test_value


async def test_inline_awaits() -> None:
    broker = InMemoryBroker(await_inplace=True)
    slept = False

    @broker.task
    async def test_task() -> None:
        nonlocal slept
        await asyncio.sleep(0.2)
        slept = True

    task = await test_task.kiq()
    assert slept
    assert await task.is_ready()
    assert not broker._running_tasks


async def test_wait_all() -> None:
    broker = InMemoryBroker()
    slept = False

    @broker.task
    async def test_task() -> None:
        nonlocal slept
        await asyncio.sleep(0.2)
        slept = True

    task = await test_task.kiq()
    assert not slept
    await broker.wait_all()
    assert slept
    assert await task.is_ready()
    assert not broker._running_tasks


async def test_wait_all_propagates_task_failure() -> None:
    task_error = LifecycleError("in-memory task failed")
    broker = InMemoryBroker()
    broker.with_middlewares(FailingExecutionMiddleware(task_error))

    @broker.task
    async def failing_task() -> None:
        return None

    await failing_task.kiq()

    with pytest.raises(LifecycleError) as exc_info:
        await broker.wait_all()

    assert exc_info.value is task_error
    assert not broker._running_tasks


async def test_wait_all_retains_failure_from_already_completed_task() -> None:
    task_error = LifecycleError("completed in-memory task failed")
    broker = InMemoryBroker()
    broker.with_middlewares(FailingExecutionMiddleware(task_error))

    @broker.task
    async def failing_task() -> None:
        return None

    await failing_task.kiq()
    while broker._running_tasks:
        await asyncio.sleep(0)

    with pytest.raises(LifecycleError) as exc_info:
        await broker.wait_all()

    assert exc_info.value is task_error
    await broker.wait_all()


async def test_shutdown_cleans_resources_after_task_failure() -> None:
    events: list[str] = []
    task_error = LifecycleError("in-memory task failed")
    broker = InMemoryBroker()
    broker.with_middlewares(RecordingLifecycleMiddleware(events))
    broker.with_middlewares(FailingExecutionMiddleware(task_error))
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


async def test_shutdown_waits_for_running_tasks_before_resource_cleanup() -> None:
    task_started = asyncio.Event()
    finish_task = asyncio.Event()
    events: list[str] = []
    broker = InMemoryBroker()
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
    await task_started.wait()

    shutdown_task = asyncio.create_task(broker.shutdown())
    await asyncio.sleep(0)

    assert not shutdown_task.done()

    finish_task.set()
    await shutdown_task

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
