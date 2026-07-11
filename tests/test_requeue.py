import asyncio

import pytest

from taskiq import (
    Context,
    Flow,
    FlowProtocol,
    InMemoryBroker,
    TaskiqDepends,
    TaskiqRouter,
)
from taskiq.acks import AckableMessage, AckController
from taskiq.exceptions import NoResultError, UnsupportedFlowError
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.receiver import Receiver
from tests.routing.models import OldStyleRecordingBroker
from tests.utils import RecordingBroker


class RequeueError(Exception):
    """Marker error for requeue lifecycle tests."""


class RequeueRecordingBroker(RecordingBroker):
    """Record publication order for receiver acknowledgement tests."""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        super().__init__()

    async def kick_to_flow(
        self,
        message: BrokerMessage,
        flow: FlowProtocol | None = None,
    ) -> None:
        """Record successful requeue publication."""
        self.events.append("publish")
        await super().kick_to_flow(message, flow)


class FailingRequeueBroker(RecordingBroker):
    """Fail requeue publication with a configured exception."""

    def __init__(self, error: BaseException) -> None:
        self.error = error
        super().__init__()

    async def kick_to_flow(
        self,
        message: BrokerMessage,
        flow: FlowProtocol | None = None,
    ) -> None:
        """Raise the configured publication error."""
        raise self.error


def build_message(task_name: str = "demo.task") -> TaskiqMessage:
    """Build a message for context requeue tests."""
    return TaskiqMessage(
        task_id="task-id",
        task_name=task_name,
        labels={},
        args=[],
        kwargs={},
    )


async def test_requeue() -> None:
    broker = InMemoryBroker()

    runs_count = 0

    @broker.task
    async def task(context: Context = TaskiqDepends()) -> None:
        nonlocal runs_count
        runs_count += 1
        if runs_count < 2:
            await context.requeue()

    kicked = await task.kiq()
    await kicked.wait_result()
    assert (
        broker.custom_dependency_context[Context].message.labels["X-Taskiq-requeue"]
        == "1"
    )

    assert runs_count == 2


async def test_requeue_from_dependency() -> None:
    broker = InMemoryBroker()

    runs_count = 0

    async def dep_func(context: Context = TaskiqDepends()) -> None:
        nonlocal runs_count
        runs_count += 1
        if runs_count < 2:
            await context.requeue()

    @broker.task
    async def task(_: None = TaskiqDepends(dep_func)) -> None:
        return None

    kicked = await task.kiq()
    await kicked.wait_result()
    assert (
        broker.custom_dependency_context[Context].message.labels["X-Taskiq-requeue"]
        == "1"
    )

    assert runs_count == 2


async def test_requeue_preserves_current_broker_when_route_points_elsewhere() -> None:
    router = TaskiqRouter()
    current_flow = Flow("current.default")
    current_broker = RecordingBroker(
        router=router,
        broker_name="current",
        default_flow=current_flow,
    )
    routed_broker = RecordingBroker(router=router, broker_name="routed")
    routed_flow = Flow("routed")
    message = build_message()
    context = Context(message, current_broker)

    router.route_task(message.task_name, broker=routed_broker, flow=routed_flow)

    with pytest.raises(NoResultError):
        await context.requeue()

    sent_message, sent_flow = current_broker.sent[0]
    assert routed_broker.sent == []
    assert sent_message.task_name == message.task_name
    assert sent_message.labels["X-Taskiq-requeue"] == "1"
    assert sent_flow == current_flow


async def test_requeue_prefers_same_broker_route_flow() -> None:
    router = TaskiqRouter()
    default_flow = Flow("current.default")
    route_flow = Flow("current.route")
    current_broker = RecordingBroker(
        router=router,
        broker_name="current",
        default_flow=default_flow,
    )
    message = build_message()
    context = Context(message, current_broker)

    router.route_task(message.task_name, broker=current_broker, flow=route_flow)

    with pytest.raises(NoResultError):
        await context.requeue()

    assert current_broker.sent[0][1] == route_flow


async def test_requeue_without_route_or_default_flow_sends_without_flow() -> None:
    broker = RecordingBroker()
    message = build_message()
    context = Context(message, broker)

    with pytest.raises(NoResultError):
        await context.requeue()

    assert broker.sent[0][1] is None


async def test_router_requeue_accepts_explicit_flow_override() -> None:
    router = TaskiqRouter()
    default_flow = Flow("default")
    explicit_flow = Flow("explicit")
    broker = RecordingBroker(
        router=router,
        broker_name="broker",
        default_flow=default_flow,
    )
    message = build_message()

    await router.requeue(message, broker=broker, flow=explicit_flow)

    assert broker.sent[0][1] == explicit_flow


async def test_requeue_unsupported_flow_keeps_original_unacked() -> None:
    router = TaskiqRouter()
    broker = OldStyleRecordingBroker(router=router, broker_name="legacy")
    flow = Flow("unsupported")
    message = build_message()
    ack_events: list[str] = []
    controller = AckController(lambda: ack_events.append("ack"))
    context = Context(message, broker, controller)
    router.route_task(message.task_name, broker=broker, flow=flow)

    with pytest.raises(UnsupportedFlowError) as exc_info:
        await context.requeue()

    assert exc_info.value.flow_name == flow.name
    assert broker.sent == []
    assert ack_events == []
    assert not context.is_acked
    assert controller.requeue_failed
    assert "X-Taskiq-requeue" not in message.labels


@pytest.mark.parametrize(
    ("ack_type", "expected_events"),
    [
        pytest.param("when_received", ["ack", "publish"], id="when-received"),
        pytest.param("when_executed", ["publish", "ack"], id="when-executed"),
        pytest.param("when_saved", ["publish", "ack"], id="when-saved"),
        pytest.param("manual", ["publish", "ack"], id="manual"),
    ],
)
async def test_receiver_requeue_settles_original_after_publication(
    ack_type: str,
    expected_events: list[str],
) -> None:
    events: list[str] = []
    broker = RequeueRecordingBroker(events)

    @broker.task(task_name="demo.requeue")
    async def demo_task(context: Context = TaskiqDepends()) -> None:
        await context.requeue()

    message = demo_task.kicker().with_labels(ack_type=ack_type).prepare().message
    broker_message = broker.formatter.dumps(message)
    receiver = Receiver(broker, max_async_tasks=1)

    await receiver.callback(
        AckableMessage(
            data=broker_message.message,
            ack=lambda: events.append("ack"),
        ),
    )

    assert events == expected_events
    assert broker.sent[0][0].labels["X-Taskiq-requeue"] == "1"


async def test_requeue_publish_failure_keeps_original_unacked_and_unchanged() -> None:
    events: list[str] = []
    broker = FailingRequeueBroker(RequeueError("publish failed"))
    message = build_message()
    controller = AckController(lambda: events.append("ack"))
    context = Context(message, broker, controller)

    with pytest.raises(RequeueError, match="publish failed"):
        await context.requeue()

    assert events == []
    assert not context.is_acked
    assert controller.requeue_failed
    assert "X-Taskiq-requeue" not in message.labels


async def test_requeue_cancellation_keeps_original_unacked_and_unchanged() -> None:
    events: list[str] = []
    cancellation = asyncio.CancelledError()
    broker = FailingRequeueBroker(cancellation)
    message = build_message()
    controller = AckController(lambda: events.append("ack"))
    context = Context(message, broker, controller)

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await context.requeue()

    assert exc_info.value is cancellation
    assert events == []
    assert not context.is_acked
    assert controller.requeue_failed
    assert "X-Taskiq-requeue" not in message.labels


@pytest.mark.parametrize("ack_type", ["when_executed", "when_saved", "manual"])
@pytest.mark.parametrize(
    "error",
    [
        pytest.param(RequeueError("publish failed"), id="error"),
        pytest.param(asyncio.CancelledError(), id="cancelled"),
    ],
)
async def test_receiver_does_not_ack_failed_requeue(
    ack_type: str,
    error: BaseException,
) -> None:
    events: list[str] = []
    broker = FailingRequeueBroker(error)

    @broker.task(task_name="demo.failed-requeue")
    async def demo_task(context: Context = TaskiqDepends()) -> None:
        await context.requeue()

    message = demo_task.kicker().with_labels(ack_type=ack_type).prepare().message
    broker_message = broker.formatter.dumps(message)
    receiver = Receiver(broker, max_async_tasks=1)

    await receiver.callback(
        AckableMessage(
            data=broker_message.message,
            ack=lambda: events.append("ack"),
        ),
    )

    context = broker.custom_dependency_context[Context]
    assert events == []
    assert not context.is_acked
    assert "X-Taskiq-requeue" not in context.message.labels


async def test_requeue_ack_failure_propagates_after_successful_publication() -> None:
    events: list[str] = []
    broker = RequeueRecordingBroker(events)

    def fail_ack() -> None:
        events.append("ack")
        raise RequeueError("ack failed")

    @broker.task(task_name="demo.ack-failure")
    async def demo_task(context: Context = TaskiqDepends()) -> None:
        await context.requeue()

    message = demo_task.kicker().with_labels(ack_type="manual").prepare().message
    broker_message = broker.formatter.dumps(message)
    receiver = Receiver(broker, max_async_tasks=1)

    with pytest.raises(RequeueError, match="ack failed"):
        await receiver.callback(
            AckableMessage(data=broker_message.message, ack=fail_ack),
        )

    context = broker.custom_dependency_context[Context]
    assert events == ["publish", "ack"]
    assert len(broker.sent) == 1
    assert context.message.labels["X-Taskiq-requeue"] == "1"
    assert not context.is_acked
