import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from taskiq.abc.schedule_source import ScheduleSource
from taskiq.abc.scheduler_plugin import SchedulerPlugin
from taskiq.api import run_scheduler_task
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.cli.scheduler.run import get_all_schedules
from taskiq.exceptions import ScheduledTaskCancelledError
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler
from tests.utils import AsyncQueueBroker


class DummyScheduleSource(ScheduleSource):
    def __init__(self, schedules: list[ScheduledTask] | None = None) -> None:
        self.schedules = schedules or []

    async def get_schedules(self) -> list["ScheduledTask"]:
        """Return schedules list."""
        return self.schedules


class CancellingScheduleSource(DummyScheduleSource):
    def pre_send(self, task: "ScheduledTask") -> None:
        """Raise cancelled error."""
        raise ScheduledTaskCancelledError


class RecordingPlugin(SchedulerPlugin):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[tuple[str, Any]] = []

    def startup(self) -> None:
        """Record startup."""
        self.events.append(("startup", None))

    async def shutdown(self) -> None:
        """Record shutdown."""
        self.events.append(("shutdown", None))

    async def on_schedules_updated(
        self,
        source: "ScheduleSource",
        schedules: list["ScheduledTask"],
    ) -> None:
        """Record schedules update."""
        self.events.append(("on_schedules_updated", (source, schedules)))

    def pre_send(self, source: "ScheduleSource", task: "ScheduledTask") -> None:
        """Record pre_send."""
        self.events.append(("pre_send", (source, task)))

    async def post_send(self, source: "ScheduleSource", task: "ScheduledTask") -> None:
        """Record post_send."""
        self.events.append(("post_send", (source, task)))


class FailingPlugin(SchedulerPlugin):
    def on_schedules_updated(
        self,
        source: "ScheduleSource",
        schedules: list["ScheduledTask"],
    ) -> None:
        """Raise an error."""
        raise ValueError("I'm a broken plugin.")

    def pre_send(self, source: "ScheduleSource", task: "ScheduledTask") -> None:
        """Raise an error."""
        raise ValueError("I'm a broken plugin.")


def get_schedule() -> ScheduledTask:
    return ScheduledTask(
        task_name="ping:pong",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )


def test_plugin_gets_scheduler() -> None:
    plugin = RecordingPlugin()
    scheduler = TaskiqScheduler(InMemoryBroker(), sources=[], plugins=[plugin])

    assert plugin.scheduler is scheduler


async def test_startup_and_shutdown_hooks() -> None:
    plugin = RecordingPlugin()
    scheduler = TaskiqScheduler(InMemoryBroker(), sources=[], plugins=[plugin])

    await scheduler.startup()
    await scheduler.shutdown()

    assert plugin.events == [("startup", None), ("shutdown", None)]


async def test_send_hooks_called() -> None:
    plugin = RecordingPlugin()
    broker = InMemoryBroker()
    source = DummyScheduleSource()
    scheduler = TaskiqScheduler(broker, sources=[source], plugins=[plugin])

    @broker.task(task_name="ping:pong")
    def _() -> None: ...

    task = get_schedule()
    await scheduler.on_ready(source, task)

    assert plugin.events == [
        ("pre_send", (source, task)),
        ("post_send", (source, task)),
    ]


async def test_send_hooks_skipped_on_cancel() -> None:
    plugin = RecordingPlugin()
    source = CancellingScheduleSource()
    scheduler = TaskiqScheduler(InMemoryBroker(), sources=[source], plugins=[plugin])

    await scheduler.on_ready(source, get_schedule())

    assert plugin.events == []


async def test_failing_plugin_does_not_break_scheduling() -> None:
    plugin = RecordingPlugin()
    broker = InMemoryBroker()
    source = DummyScheduleSource()
    scheduler = TaskiqScheduler(
        broker,
        sources=[source],
        plugins=[FailingPlugin(), plugin],
    )

    @broker.task(task_name="ping:pong")
    def _() -> None: ...

    task = get_schedule()
    await scheduler.on_ready(source, task)

    assert plugin.events == [
        ("pre_send", (source, task)),
        ("post_send", (source, task)),
    ]


async def test_schedules_updated_hook() -> None:
    plugin = RecordingPlugin()
    schedule = get_schedule()
    source = DummyScheduleSource([schedule])
    scheduler = TaskiqScheduler(InMemoryBroker(), sources=[source], plugins=[plugin])

    await get_all_schedules(scheduler)

    assert plugin.events == [("on_schedules_updated", (source, [schedule]))]


async def test_plugin_in_scheduler_loop() -> None:
    plugin = RecordingPlugin()
    broker = AsyncQueueBroker()
    schedule = ScheduledTask(
        task_name="ping:pong",
        labels={},
        args=[],
        kwargs={},
        time=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    scheduler = TaskiqScheduler(
        broker,
        sources=[DummyScheduleSource([schedule])],
        plugins=[plugin],
    )

    @broker.task(task_name="ping:pong")
    def _() -> None: ...

    scheduler_task = asyncio.create_task(run_scheduler_task(scheduler))
    msg = await asyncio.wait_for(broker.queue.get(), 2)
    assert msg
    scheduler_task.cancel()

    hooks = [event[0] for event in plugin.events]
    assert "on_schedules_updated" in hooks
    assert "pre_send" in hooks
    assert "post_send" in hooks
