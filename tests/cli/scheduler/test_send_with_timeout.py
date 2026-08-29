import asyncio
import logging
from typing import Any

import pytest

from taskiq.abc.schedule_source import ScheduleSource
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.cli.scheduler.run import send, send_with_timeout
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler


class _StubSource(ScheduleSource):
    async def get_schedules(self) -> list[ScheduledTask]:
        return []


class _HangingScheduler(TaskiqScheduler):
    """Test double whose ``on_ready`` blocks longer than any reasonable timeout.

    Models the production failure mode: ``broker.kick`` (deep inside ``on_ready``)
    hangs on a stalled Redis socket and never returns. The scheduler's spawned
    ``send`` task would normally retain its ``running_schedules`` entry forever,
    silently skipping every subsequent tick.
    """

    def __init__(self, hang_seconds: float = 60.0) -> None:
        super().__init__(broker=InMemoryBroker(), sources=[_StubSource()])
        self.hang_seconds = hang_seconds
        self.on_ready_calls = 0

    async def on_ready(  # type: ignore[override]
        self, source: ScheduleSource, task: ScheduledTask
    ) -> None:
        self.on_ready_calls += 1
        await asyncio.sleep(self.hang_seconds)


def _task() -> ScheduledTask:
    return ScheduledTask(
        task_name="dummy",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
        schedule_id="dummy-schedule-id",
    )


@pytest.mark.anyio
async def test_send_with_timeout_returns_on_timeout_without_raising(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A send that exceeds the timeout must be cancelled cleanly and NOT raise.

    Raising out of the spawned task would propagate to the
    ``add_done_callback``, but the callback only cares that the task completed;
    we still want the entry removed from ``running_schedules`` so the next
    cron boundary can retry. Returning normally is the right shape.
    """
    caplog.set_level(logging.WARNING, logger="taskiq.cli.scheduler.run")

    scheduler = _HangingScheduler(hang_seconds=30.0)
    task = _task()
    source = _StubSource()

    # Must complete promptly (within ~0.1s) and NOT raise.
    await asyncio.wait_for(
        send_with_timeout(scheduler, source, task, timeout=0.05),
        timeout=2.0,
    )

    assert scheduler.on_ready_calls == 1

    warnings = [
        rec
        for rec in caplog.records
        if rec.name == "taskiq.cli.scheduler.run" and rec.levelno >= logging.WARNING
    ]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "timed out" in msg
    assert "dummy-schedule-id" in msg
    assert "dummy" in msg


@pytest.mark.anyio
async def test_send_with_timeout_does_not_log_on_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A send that completes well within the timeout must not produce a warning."""
    caplog.set_level(logging.WARNING, logger="taskiq.cli.scheduler.run")

    class _FastScheduler(TaskiqScheduler):
        def __init__(self) -> None:
            super().__init__(broker=InMemoryBroker(), sources=[_StubSource()])
            self.calls = 0

        async def on_ready(  # type: ignore[override]
            self, source: ScheduleSource, task: ScheduledTask
        ) -> None:
            self.calls += 1

    scheduler = _FastScheduler()
    task = _task()
    source = _StubSource()

    await send_with_timeout(scheduler, source, task, timeout=5.0)

    assert scheduler.calls == 1
    warnings = [
        rec
        for rec in caplog.records
        if rec.name == "taskiq.cli.scheduler.run" and rec.levelno >= logging.WARNING
    ]
    assert warnings == []


@pytest.mark.anyio
async def test_send_with_timeout_propagates_non_timeout_exceptions() -> None:
    """Errors inside on_ready that AREN'T a timeout must still propagate.

    The wrapper only swallows asyncio.TimeoutError. Other exceptions (e.g.,
    SendTaskError raised by a broker) need to surface to the existing logging
    and observability surfaces.
    """

    class _BoomScheduler(TaskiqScheduler):
        def __init__(self) -> None:
            super().__init__(broker=InMemoryBroker(), sources=[_StubSource()])

        async def on_ready(  # type: ignore[override]
            self, source: ScheduleSource, task: ScheduledTask
        ) -> None:
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await send_with_timeout(_BoomScheduler(), _StubSource(), _task(), timeout=5.0)


@pytest.mark.anyio
async def test_send_with_timeout_cancels_inner_send() -> None:
    """The inner ``send`` coroutine must actually be cancelled on timeout.

    Without this guarantee, a hung send could continue executing in the
    background and double-fire when the next tick spawns a fresh send.
    """
    cancelled = asyncio.Event()

    class _CancelObservingScheduler(TaskiqScheduler):
        def __init__(self) -> None:
            super().__init__(broker=InMemoryBroker(), sources=[_StubSource()])

        async def on_ready(  # type: ignore[override]
            self, source: ScheduleSource, task: ScheduledTask
        ) -> None:
            try:
                await asyncio.sleep(30.0)
            except asyncio.CancelledError:
                cancelled.set()
                raise

    await send_with_timeout(
        _CancelObservingScheduler(),
        _StubSource(),
        _task(),
        timeout=0.05,
    )
    # Give the event loop a chance to deliver the cancellation completion.
    await asyncio.wait_for(cancelled.wait(), timeout=2.0)


@pytest.mark.anyio
async def test_plain_send_still_works_unchanged() -> None:
    """The original ``send`` function must remain unchanged in behavior.

    The timeout path is opt-in via ``send_with_timeout`` only.
    """

    class _RecordingScheduler(TaskiqScheduler):
        def __init__(self) -> None:
            super().__init__(broker=InMemoryBroker(), sources=[_StubSource()])
            self.calls: list[Any] = []

        async def on_ready(  # type: ignore[override]
            self, source: ScheduleSource, task: ScheduledTask
        ) -> None:
            self.calls.append((source, task))

    scheduler = _RecordingScheduler()
    task = _task()
    source = _StubSource()

    await send(scheduler, source, task)

    assert len(scheduler.calls) == 1
    assert scheduler.calls[0][1].schedule_id == "dummy-schedule-id"
