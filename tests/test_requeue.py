import pytest

from taskiq import Context, Flow, InMemoryBroker, TaskiqDepends, TaskiqRouter
from taskiq.exceptions import NoResultError
from taskiq.message import TaskiqMessage
from tests.utils import RecordingBroker


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
