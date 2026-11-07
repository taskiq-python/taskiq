import asyncio
from contextlib import suppress
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest

from taskiq import BrokerMessage, ScheduleSource, TaskiqScheduler
from taskiq.api import run_scheduler_task
from taskiq.cli.scheduler.args import SchedulerArgs
from taskiq.cli.scheduler.run import SchedulerLoop, run_scheduler
from taskiq.scheduler.scheduled_task import ScheduledTask
from tests.utils import AsyncQueueBroker


class PendingOperation:
    def __init__(self, cleanup_allowed: asyncio.Event) -> None:
        self.started = asyncio.Event()
        self.cleanup_started = asyncio.Event()
        self.finished = asyncio.Event()
        self.cleanup_allowed = cleanup_allowed
        self.task: asyncio.Task[Any] | None = None

    async def run(self) -> None:
        self.task = asyncio.current_task()
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cleanup_started.set()
            await self.cleanup_allowed.wait()
            self.finished.set()


@pytest.mark.parametrize(
    ("entrypoint", "send_timeout"),
    [("loop", None), ("api", None), ("cli", None), ("loop", 60), ("cli", 60)],
)
async def test_shutdown_drains_sends_and_updates(
    entrypoint: str,
    send_timeout: float | None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Owned operations must finish cleanup before scheduler resources close."""
    cleanup_allowed = asyncio.Event()
    sending = PendingOperation(cleanup_allowed)
    updating = PendingOperation(cleanup_allowed)
    shutdowns: list[str] = []

    class BlockingBroker(AsyncQueueBroker):
        async def kick(self, message: BrokerMessage) -> None:
            await sending.run()

        async def shutdown(self) -> None:
            shutdowns.append("broker")
            assert sending.finished.is_set()
            assert updating.finished.is_set()
            await super().shutdown()

    class BlockingSource(ScheduleSource):
        fetched = False

        async def get_schedules(self) -> list[ScheduledTask]:
            if self.fetched:
                await updating.run()
            self.fetched = True
            return [
                ScheduledTask(
                    task_name="test",
                    labels={},
                    args=[],
                    kwargs={},
                    interval=1,
                ),
            ]

        async def shutdown(self) -> None:
            shutdowns.append("source")
            assert updating.finished.is_set()

    monkeypatch.setattr(
        "taskiq.cli.scheduler.run._sleep_until_next_second",
        AsyncMock(),
    )
    scheduler = TaskiqScheduler(BlockingBroker(), [BlockingSource()])
    if entrypoint == "cli":
        coroutine = run_scheduler(
            SchedulerArgs(
                scheduler=scheduler,
                modules=[],
                update_interval=0,
                configure_logging=False,
                send_timeout=send_timeout,
            ),
        )
    elif entrypoint == "api":
        coroutine = run_scheduler_task(scheduler, interval=timedelta(0))
    else:
        coroutine = SchedulerLoop(scheduler).run(
            update_interval=timedelta(0),
            send_timeout=send_timeout,
        )

    scheduler_task = asyncio.create_task(coroutine)
    cleanup_started = asyncio.gather(
        sending.cleanup_started.wait(),
        updating.cleanup_started.wait(),
    )
    try:
        await asyncio.wait_for(
            asyncio.gather(sending.started.wait(), updating.started.wait()),
            5,
        )
        scheduler_task.cancel()
        waitables: list[asyncio.Future[Any]] = [scheduler_task, cleanup_started]
        done, _ = await asyncio.wait(
            waitables,
            timeout=5,
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert cleanup_started in done
        assert not scheduler_task.done()
        assert not shutdowns

        cleanup_allowed.set()
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(scheduler_task, 5)

        assert sending.finished.is_set()
        assert updating.finished.is_set()
        assert scheduler_task.cancelled() == (entrypoint != "cli")
        assert shutdowns == (["broker", "source"] if entrypoint == "cli" else [])
    finally:
        cleanup_allowed.set()
        tasks = [
            task
            for task in (scheduler_task, sending.task, updating.task)
            if task is not None
        ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        cleanup_started.cancel()
        await asyncio.gather(cleanup_started, return_exceptions=True)

    assert "Exception in callback" not in caplog.text
