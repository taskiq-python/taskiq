import asyncio
from unittest.mock import AsyncMock

import pytest

from taskiq import ScheduleSource, TaskiqScheduler
from taskiq.abc.broker import AsyncBroker
from taskiq.cli.scheduler.args import SchedulerArgs
from taskiq.cli.scheduler.run import SchedulerLoop, run_scheduler
from taskiq.scheduler.scheduled_task import ScheduledTask


class RecordingSource(ScheduleSource):
    def __init__(
        self,
        name: str,
        events: list[str],
        startup_error: BaseException | None = None,
        shutdown_error: BaseException | None = None,
    ) -> None:
        self.name = name
        self.events = events
        self.startup_error = startup_error
        self.shutdown_error = shutdown_error

    async def startup(self) -> None:
        self.events.append(f"start {self.name}")
        if self.startup_error is not None:
            raise self.startup_error

    async def shutdown(self) -> None:
        await asyncio.sleep(0)
        self.events.append(f"stop {self.name}")
        if self.shutdown_error is not None:
            raise self.shutdown_error

    async def get_schedules(self) -> list[ScheduledTask]:
        return []


@pytest.fixture
def broker() -> AsyncMock:
    return AsyncMock(spec=AsyncBroker)


@pytest.fixture
def loop_run(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    run = AsyncMock(side_effect=asyncio.CancelledError)
    monkeypatch.setattr(SchedulerLoop, "run", run)
    return run


@pytest.mark.parametrize("failed_index", [0, 1, 2])
async def test_source_startup_failure(
    broker: AsyncMock,
    loop_run: AsyncMock,
    failed_index: int,
) -> None:
    events: list[str] = []
    error = RuntimeError("source unavailable")
    sources = [RecordingSource(str(index), events) for index in range(3)]
    sources[failed_index].startup_error = error
    scheduler = TaskiqScheduler(broker, sources=list(sources))

    with pytest.raises(RuntimeError) as exc_info:
        await run_scheduler(SchedulerArgs(scheduler=scheduler, modules=[]))

    assert exc_info.value is error
    assert events == [f"start {index}" for index in range(failed_index + 1)] + [
        f"stop {index}" for index in reversed(range(failed_index))
    ]
    broker.startup.assert_not_awaited()
    broker.shutdown.assert_not_awaited()
    loop_run.assert_not_awaited()


@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError])
async def test_scheduler_startup_failure(
    broker: AsyncMock,
    loop_run: AsyncMock,
    error_type: type[BaseException],
) -> None:
    events: list[str] = []
    error = error_type("broker unavailable")
    broker.startup.side_effect = error
    scheduler = TaskiqScheduler(
        broker,
        sources=[RecordingSource("a", events), RecordingSource("b", events)],
    )

    with pytest.raises(error_type) as exc_info:
        await run_scheduler(SchedulerArgs(scheduler=scheduler, modules=[]))

    assert exc_info.value is error
    assert events == ["start a", "start b", "stop b", "stop a"]
    broker.startup.assert_awaited_once()
    broker.shutdown.assert_not_awaited()
    loop_run.assert_not_awaited()


@pytest.mark.parametrize("cleanup_error_type", [RuntimeError, asyncio.CancelledError])
async def test_cleanup_failure_preserves_startup_error(
    broker: AsyncMock,
    loop_run: AsyncMock,
    caplog: pytest.LogCaptureFixture,
    cleanup_error_type: type[BaseException],
) -> None:
    events: list[str] = []
    error = RuntimeError("source unavailable")
    cleanup_error = cleanup_error_type("cleanup failed")
    scheduler = TaskiqScheduler(
        broker,
        sources=[
            RecordingSource("a", events),
            RecordingSource("b", events, shutdown_error=cleanup_error),
            RecordingSource("c", events, startup_error=error),
        ],
    )

    with pytest.raises(RuntimeError) as exc_info:
        await run_scheduler(SchedulerArgs(scheduler=scheduler, modules=[]))

    assert exc_info.value is error
    assert events == ["start a", "start b", "start c", "stop b", "stop a"]
    assert any(
        record.exc_info is not None and record.exc_info[1] is cleanup_error
        for record in caplog.records
    )
    broker.shutdown.assert_not_awaited()
    loop_run.assert_not_awaited()


async def test_cancel_during_source_startup(
    broker: AsyncMock,
    loop_run: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    starting = asyncio.Event()
    source = RecordingSource("b", events)

    async def blocked_startup() -> None:
        events.append("start b")
        starting.set()
        await asyncio.Future[None]()

    monkeypatch.setattr(source, "startup", blocked_startup)
    scheduler = TaskiqScheduler(
        broker,
        sources=[RecordingSource("a", events), source, RecordingSource("c", events)],
    )
    task = asyncio.create_task(
        run_scheduler(SchedulerArgs(scheduler=scheduler, modules=[])),
    )
    try:
        await asyncio.wait_for(starting.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()
    assert events == ["start a", "start b", "stop a"]
    broker.startup.assert_not_awaited()
    broker.shutdown.assert_not_awaited()
    loop_run.assert_not_awaited()


async def test_normal_shutdown(broker: AsyncMock, loop_run: AsyncMock) -> None:
    events: list[str] = []
    broker.startup.side_effect = lambda: events.append("start broker")
    broker.shutdown.side_effect = lambda: events.append("stop broker")
    scheduler = TaskiqScheduler(
        broker,
        sources=[RecordingSource("a", events), RecordingSource("b", events)],
    )

    await run_scheduler(SchedulerArgs(scheduler=scheduler, modules=[]))

    assert events == [
        "start a",
        "start b",
        "start broker",
        "stop broker",
        "stop a",
        "stop b",
    ]
    broker.startup.assert_awaited_once()
    broker.shutdown.assert_awaited_once()
    loop_run.assert_awaited_once()
