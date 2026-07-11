import asyncio
from datetime import datetime, timezone

import pytest

from taskiq import Flow, TaskiqRouter
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.compat import model_dump
from taskiq.exceptions import ScheduledTaskCancelledError
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler
from tests.utils import RecordingBroker


class LifecycleError(RuntimeError):
    """Marker error for scheduler broker lifecycle tests."""


class LifecycleBroker(RecordingBroker):
    """Recording broker with observable startup and shutdown behavior."""

    def __init__(
        self,
        events: list[str],
        *,
        router: TaskiqRouter,
        broker_name: str,
        startup_error: BaseException | None = None,
        shutdown_error: BaseException | None = None,
    ) -> None:
        self.events = events
        self.startup_error = startup_error
        self.shutdown_error = shutdown_error
        super().__init__(router=router, broker_name=broker_name)

    async def startup(self) -> None:
        """Record startup and raise the configured failure."""
        self.events.append(f"startup:{self.broker_name}")
        if self.startup_error is not None:
            raise self.startup_error
        await super().startup()

    async def shutdown(self) -> None:
        """Record shutdown and raise the configured failure."""
        self.events.append(f"shutdown:{self.broker_name}")
        await super().shutdown()
        if self.shutdown_error is not None:
            raise self.shutdown_error


class RecordingScheduleSource(ScheduleSource):
    """Schedule source that records dynamically added schedules."""

    def __init__(self) -> None:
        self.schedules: list[ScheduledTask] = []

    async def get_schedules(self) -> list["ScheduledTask"]:
        """Return stored schedules."""
        return self.schedules

    async def add_schedule(self, schedule: "ScheduledTask") -> None:
        """Store a schedule."""
        self.schedules.append(schedule)


class CancellingScheduleSource(ScheduleSource):
    async def get_schedules(self) -> list["ScheduledTask"]:
        """Return schedules list."""
        return []

    def pre_send(
        self,
        task: "ScheduledTask",
    ) -> None:
        """Raise cancelled error."""
        raise ScheduledTaskCancelledError


async def test_scheduler_owns_all_router_broker_lifecycles() -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = LifecycleBroker(events, router=router, broker_name="first")
    second = LifecycleBroker(events, router=router, broker_name="second")
    scheduler = TaskiqScheduler(broker=first, sources=[])

    await scheduler.startup()

    assert first.is_scheduler_process
    assert second.is_scheduler_process
    assert events == ["startup:first", "startup:second"]

    await scheduler.shutdown()

    assert events == [
        "startup:first",
        "startup:second",
        "shutdown:second",
        "shutdown:first",
    ]


async def test_scheduler_cleans_up_partial_broker_startup() -> None:
    events: list[str] = []
    router = TaskiqRouter()
    cleanup_error = LifecycleError("first cleanup failed")
    first = LifecycleBroker(
        events,
        router=router,
        broker_name="first",
        shutdown_error=cleanup_error,
    )
    startup_error = LifecycleError("second startup failed")
    LifecycleBroker(
        events,
        router=router,
        broker_name="second",
        startup_error=startup_error,
    )
    LifecycleBroker(events, router=router, broker_name="not-started")
    scheduler = TaskiqScheduler(broker=first, sources=[])

    with pytest.raises(LifecycleError) as exc_info:
        await scheduler.startup()

    assert exc_info.value is startup_error

    assert events == [
        "startup:first",
        "startup:second",
        "shutdown:second",
        "shutdown:first",
    ]


async def test_scheduler_shutdown_after_failed_startup_does_not_repeat_cleanup() -> (
    None
):
    events: list[str] = []
    router = TaskiqRouter()
    first = LifecycleBroker(events, router=router, broker_name="first")
    LifecycleBroker(
        events,
        router=router,
        broker_name="second",
        startup_error=LifecycleError("second startup failed"),
    )
    scheduler = TaskiqScheduler(broker=first, sources=[])

    with pytest.raises(LifecycleError, match="second startup failed"):
        await scheduler.startup()

    events_after_rollback = list(events)
    await scheduler.shutdown()

    assert events == events_after_rollback


async def test_scheduler_shutdown_closes_all_brokers_after_failure() -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = LifecycleBroker(
        events,
        router=router,
        broker_name="first",
        shutdown_error=LifecycleError("first shutdown failed"),
    )
    second_error = LifecycleError("second shutdown failed")
    LifecycleBroker(
        events,
        router=router,
        broker_name="second",
        shutdown_error=second_error,
    )
    LifecycleBroker(events, router=router, broker_name="third")
    scheduler = TaskiqScheduler(broker=first, sources=[])
    await scheduler.startup()

    with pytest.raises(LifecycleError) as exc_info:
        await scheduler.shutdown()

    assert exc_info.value is second_error

    assert events[-3:] == [
        "shutdown:third",
        "shutdown:second",
        "shutdown:first",
    ]


async def test_scheduler_rejects_duplicate_startup() -> None:
    events: list[str] = []
    router = TaskiqRouter()
    broker = LifecycleBroker(events, router=router, broker_name="broker")
    scheduler = TaskiqScheduler(broker=broker, sources=[])
    await scheduler.startup()

    with pytest.raises(RuntimeError, match="already started"):
        await scheduler.startup()

    await scheduler.shutdown()
    assert events == ["startup:broker", "shutdown:broker"]


async def test_scheduler_rejects_shutdown_while_startup_is_in_progress() -> None:
    events: list[str] = []
    startup_started = asyncio.Event()
    release_startup = asyncio.Event()

    class BlockingStartupBroker(LifecycleBroker):
        async def startup(self) -> None:
            startup_started.set()
            await release_startup.wait()
            await super().startup()

    router = TaskiqRouter()
    broker = BlockingStartupBroker(events, router=router, broker_name="broker")
    scheduler = TaskiqScheduler(broker=broker, sources=[])
    startup_task = asyncio.create_task(scheduler.startup())
    await startup_started.wait()

    with pytest.raises(RuntimeError, match="still starting"):
        await scheduler.shutdown()

    release_startup.set()
    await startup_task
    await scheduler.shutdown()


async def test_scheduler_rejects_operations_while_shutdown_is_in_progress() -> None:
    events: list[str] = []
    shutdown_started = asyncio.Event()
    release_shutdown = asyncio.Event()

    class BlockingShutdownBroker(LifecycleBroker):
        async def shutdown(self) -> None:
            shutdown_started.set()
            await release_shutdown.wait()
            await super().shutdown()

    router = TaskiqRouter()
    broker = BlockingShutdownBroker(events, router=router, broker_name="broker")
    scheduler = TaskiqScheduler(broker=broker, sources=[])
    await scheduler.startup()
    shutdown_task = asyncio.create_task(scheduler.shutdown())
    await shutdown_started.wait()

    with pytest.raises(RuntimeError, match="shutting down"):
        await scheduler.startup()
    with pytest.raises(RuntimeError, match="already shutting down"):
        await scheduler.shutdown()

    release_shutdown.set()
    await shutdown_task


async def test_scheduler_shutdown_before_startup_preserves_legacy_behavior() -> None:
    events: list[str] = []
    router = TaskiqRouter()
    broker = LifecycleBroker(events, router=router, broker_name="broker")
    scheduler = TaskiqScheduler(broker=broker, sources=[])

    await scheduler.shutdown()

    assert events == ["shutdown:broker"]


async def test_scheduled_task_cancelled() -> None:
    broker = InMemoryBroker()
    source = CancellingScheduleSource()
    scheduler = TaskiqScheduler(broker=broker, sources=[source])
    task = ScheduledTask(
        task_name="ping:pong",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )

    await scheduler.on_ready(source, task)  # error is caught


async def test_scheduler_resolves_route_at_send_time() -> None:
    router = TaskiqRouter()
    scheduler_broker = RecordingBroker(router=router, broker_name="scheduler")
    first_broker = RecordingBroker(router=router, broker_name="first")
    second_broker = RecordingBroker(router=router, broker_name="second")
    first_flow = Flow("first")
    second_flow = Flow("second")
    source = RecordingScheduleSource()
    scheduler = TaskiqScheduler(broker=scheduler_broker, sources=[source])

    @scheduler_broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    router.route_task(demo_task, broker=first_broker, flow=first_flow)
    scheduled_task = ScheduledTask(
        task_name="demo.task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    router.route_task(demo_task, broker=second_broker, flow=second_flow)

    await scheduler.on_ready(source, scheduled_task)

    assert first_broker.sent == []
    assert second_broker.sent[0][0].task_name == "demo.task"
    assert second_broker.sent[0][1] == second_flow


async def test_scheduler_without_route_uses_scheduler_broker() -> None:
    router = TaskiqRouter()
    default_broker = RecordingBroker(router=router, broker_name="default")
    scheduler_flow = Flow("scheduler.default")
    scheduler_broker = RecordingBroker(
        router=router,
        broker_name="scheduler",
        default_flow=scheduler_flow,
    )
    source = RecordingScheduleSource()
    scheduler = TaskiqScheduler(broker=scheduler_broker, sources=[source])
    scheduled_task = ScheduledTask(
        task_name="external.task",
        labels={"source": "database"},
        args=[],
        kwargs={},
        cron="* * * * *",
    )

    await scheduler.on_ready(source, scheduled_task)

    sent_message, sent_flow = scheduler_broker.sent[0]
    assert default_broker.sent == []
    assert sent_message.task_name == "external.task"
    assert sent_message.labels["source"] == "database"
    assert sent_message.labels["schedule_id"] == scheduled_task.schedule_id
    assert sent_flow == scheduler_flow
    assert scheduled_task.labels == {"source": "database"}


async def test_scheduler_removed_route_uses_scheduler_broker() -> None:
    router = TaskiqRouter()
    scheduler_flow = Flow("scheduler.default")
    scheduler_broker = RecordingBroker(
        router=router,
        broker_name="scheduler",
        default_flow=scheduler_flow,
    )
    target_broker = RecordingBroker(router=router, broker_name="target")
    source = RecordingScheduleSource()
    scheduler = TaskiqScheduler(broker=scheduler_broker, sources=[source])

    @scheduler_broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    router.route_task(demo_task, broker=target_broker, flow=Flow("target"))
    scheduled_task = ScheduledTask(
        task_name="demo.task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )
    removed_route = router.remove_route("demo.task")

    assert removed_route is not None
    assert removed_route.broker is target_broker

    await scheduler.on_ready(source, scheduled_task)

    sent_message, sent_flow = scheduler_broker.sent[0]
    assert target_broker.sent == []
    assert sent_message.task_name == "demo.task"
    assert sent_flow == scheduler_flow


async def test_created_schedule_kiq_does_not_mutate_schedule_payload() -> None:
    router = TaskiqRouter()
    source_broker = RecordingBroker(router=router, broker_name="source")
    target_broker = RecordingBroker(router=router, broker_name="target")
    target_flow = Flow("target")
    source = RecordingScheduleSource()

    @source_broker.task(task_name="demo.task")
    async def demo_task(value: int) -> None:
        return None

    route = router.resolve_route(
        demo_task,
        broker=target_broker,
        flow=target_flow,
    )
    schedule = (
        await demo_task.kicker()
        .with_route(route)
        .with_labels(trace_id="trace")
        .schedule_by_time(source, datetime.now(timezone.utc), 1)
    )
    schedule_payload = model_dump(schedule.task)

    await schedule.kiq()

    assert source.schedules == [schedule.task]
    assert {"route", "flow", "broker", "broker_name"}.isdisjoint(schedule_payload)
    assert schedule.task.labels == {"trace_id": "trace"}
    assert target_broker.sent[0][0].task_name == "demo.task"
    assert target_broker.sent[0][1] == target_flow
