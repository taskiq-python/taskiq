from datetime import datetime, timezone

from taskiq import Flow, TaskiqRouter
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.compat import model_dump
from taskiq.exceptions import ScheduledTaskCancelledError
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler
from tests.utils import RecordingBroker


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
