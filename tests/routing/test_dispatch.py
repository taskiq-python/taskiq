import pytest

from taskiq import (
    Flow,
    FlowProtocol,
    InMemoryBroker,
    TaskiqRoute,
    TaskiqRouter,
    UnsupportedFlowError,
)
from tests.routing.models import (
    BrokerQueue,
    OldStyleRecordingBroker,
    RecordingMiddleware,
    RecordingResultBackend,
)
from tests.utils import RecordingBroker


async def test_old_broker_task_api_registers_task_in_router() -> None:
    broker = RecordingBroker()

    @broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    assert broker.find_task("demo.task") is demo_task
    assert broker.router.find_task("demo.task") is demo_task

    await demo_task.kiq()

    assert broker.sent[0][0].task_name == "demo.task"
    assert broker.sent[0][1] is None


async def test_broker_task_api_uses_default_flow() -> None:
    flow = Flow("default")
    broker = RecordingBroker(default_flow=flow)

    @broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    await demo_task.kiq()

    assert broker.sent[0][0].task_name == "demo.task"
    assert broker.sent[0][1] == flow
    assert broker.get_subscribed_flows() == (flow,)


async def test_registered_task_resolves_current_default_flow_at_send_time() -> None:
    first_flow = Flow("first")
    second_flow = Flow("second")
    broker = RecordingBroker(default_flow=first_flow)

    @broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    initial_route = broker.router.resolve_route(demo_task)
    broker.default_flow = second_flow

    await demo_task.kiq()
    await demo_task.kicker().with_route(initial_route).kiq()

    assert initial_route.flow == first_flow
    assert broker.router.resolve_route(demo_task).flow == second_flow
    assert broker.router.routes[demo_task.task_name].flow == second_flow
    assert [flow for _, flow in broker.sent] == [second_flow, first_flow]


async def test_explicit_task_route_does_not_follow_default_flow_changes() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(
        router=router,
        broker_name="broker",
        default_flow=Flow("default.first"),
    )
    explicit_flow = Flow("explicit")

    @broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    router.route_task(demo_task, broker=broker, flow=explicit_flow)
    broker.default_flow = Flow("default.second")

    await demo_task.kiq()

    assert router.resolve_route(demo_task).flow == explicit_flow
    assert broker.sent[0][1] == explicit_flow


def test_router_set_broker_can_configure_default_flow() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")
    flow = Flow("manual.default")

    router.set_broker(broker, name="broker", default_flow=flow)
    route = router.resolve_route("demo.task", broker=broker)

    assert broker.default_flow == flow
    assert route.flow == flow
    assert broker.get_subscribed_flows() == (flow,)


async def test_router_can_route_task_to_another_broker_flow() -> None:
    router = TaskiqRouter()
    source = RecordingBroker(router=router, broker_name="source")
    target = RecordingBroker(router=router, broker_name="target")
    flow = Flow("events")

    @source.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    route = router.route_task(demo_task, broker=target, flow=flow)
    resolved_route = router.resolve_route(demo_task)

    await demo_task.kiq()

    assert route.broker is target
    assert route.broker_name == "target"
    assert resolved_route.broker is target
    assert resolved_route.flow == flow
    assert source.sent == []
    assert target.sent[0][0].task_name == "demo.task"
    assert target.sent[0][1] == flow


async def test_old_style_broker_uses_fallback_without_flow() -> None:
    router = TaskiqRouter()
    source = RecordingBroker(router=router, broker_name="source")
    target = OldStyleRecordingBroker(router=router, broker_name="target")

    @source.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    router.route_task(demo_task, broker=target)

    await demo_task.kiq()

    assert source.sent == []
    assert target.sent[0].task_name == "demo.task"


async def test_old_style_broker_rejects_explicit_flow() -> None:
    router = TaskiqRouter()
    source = RecordingBroker(router=router, broker_name="source")
    target = OldStyleRecordingBroker(router=router, broker_name="target")
    flow = Flow("legacy")

    @source.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    router.route_task(demo_task, broker=target, flow=flow)

    with pytest.raises(
        UnsupportedFlowError,
        match="OldStyleRecordingBroker does not support explicit flow 'legacy'",
    ) as exc_info:
        await demo_task.kiq()

    assert exc_info.value.broker_type == "OldStyleRecordingBroker"
    assert exc_info.value.flow_name == "legacy"
    assert source.sent == []
    assert target.sent == []


async def test_kicker_route_override_wins_over_registered_route() -> None:
    router = TaskiqRouter()
    first = RecordingBroker(router=router, broker_name="first")
    second = RecordingBroker(router=router, broker_name="second")
    first_flow = Flow("first")
    second_flow = Flow("second")

    @first.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    router.route_task(demo_task, broker=first, flow=first_flow)

    route = router.resolve_route(demo_task, broker=second, flow=second_flow)

    await demo_task.kicker().with_route(route).kiq()

    assert first.sent == []
    assert second.sent[0][1] == second_flow


async def test_kicker_broker_override_wins_over_registered_route() -> None:
    router = TaskiqRouter()
    first = RecordingBroker(router=router, broker_name="first")
    second_flow = Flow("second")
    second = RecordingBroker(
        router=router,
        broker_name="second",
        default_flow=second_flow,
    )
    first_flow = Flow("first")

    @first.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    first_route = router.route_task(demo_task, broker=first, flow=first_flow)

    await demo_task.kicker().with_route(first_route).with_broker(second).kiq()

    assert first.sent == []
    assert second.sent[0][1] == second_flow


async def test_kicker_broker_override_clears_earlier_flow_override() -> None:
    router = TaskiqRouter()
    source = RecordingBroker(router=router, broker_name="source")
    target_flow = Flow("target.default")
    target = RecordingBroker(
        router=router,
        broker_name="target",
        default_flow=target_flow,
    )

    @source.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    await demo_task.kicker().with_flow(Flow("stale")).with_broker(target).kiq()

    assert source.sent == []
    assert target.sent[0][1] == target_flow


async def test_kicker_flow_override_after_broker_override_wins() -> None:
    router = TaskiqRouter()
    source = RecordingBroker(router=router, broker_name="source")
    target = RecordingBroker(
        router=router,
        broker_name="target",
        default_flow=Flow("target.default"),
    )
    explicit_flow = Flow("target.explicit")

    @source.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    await demo_task.kicker().with_broker(target).with_flow(explicit_flow).kiq()

    assert source.sent == []
    assert target.sent[0][1] == explicit_flow


def test_kicker_rejects_ambiguous_none_flow_override() -> None:
    broker = RecordingBroker(default_flow=Flow("default"))

    @broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    with pytest.raises(TypeError, match="flow cannot be None"):
        demo_task.kicker().with_flow(None)  # type: ignore[arg-type]


async def test_explicit_route_can_bypass_broker_default_flow() -> None:
    broker = RecordingBroker(default_flow=Flow("default"))

    @broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    await demo_task.kicker().with_route(TaskiqRoute(broker, flow=None)).kiq()

    assert broker.sent[0][1] is None


async def test_router_uses_explicit_broker_lookup_for_config_names() -> None:
    router = TaskiqRouter()
    source = RecordingBroker(router=router, broker_name="source")
    target = RecordingBroker(router=router, broker_name="target")
    flow = Flow("compat")

    @source.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    broker = router.get_broker("target")
    route = router.route_task("demo.task", broker=broker, flow=flow)

    await demo_task.kicker().with_route(route).kiq()

    assert target.sent[0][0].task_name == "demo.task"
    assert target.sent[0][1] == flow


async def test_router_accepts_broker_specific_flow_protocol() -> None:
    broker = RecordingBroker(broker_name="transport")
    flow = BrokerQueue(name="critical", durable=False)

    @broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    await demo_task.kicker().with_flow(flow).kiq()

    assert isinstance(flow, FlowProtocol)
    assert broker.sent[0][1] is flow
    assert flow.broker_options() == {
        "durable": False,
    }


async def test_kicker_preserves_labels_task_id_and_result_backend() -> None:
    broker = RecordingBroker()
    backend = RecordingResultBackend()
    broker.with_id_generator(lambda: "generated-id")
    broker.with_result_backend(backend)

    @broker.task(task_name="demo.task", task_label="declared")
    async def demo_task() -> None:
        return None

    sent_task = await demo_task.kicker().with_labels(call_label=42).kiq()
    broker_message = broker.sent[0][0]

    assert sent_task.task_id == "generated-id"
    assert sent_task.result_backend is backend
    assert broker_message.task_id == "generated-id"
    assert broker_message.labels["task_label"] == "declared"
    assert broker_message.labels["call_label"] == "42"


async def test_kicker_can_prepare_invocation_for_later() -> None:
    broker = RecordingBroker()

    @broker.task(task_name="demo.task")
    async def demo_task(value: int) -> None:
        return None

    prepared = demo_task.kicker().with_labels(trace_id="abc").prepare(1)

    assert prepared.message.task_name == "demo.task"
    assert prepared.message.args == [1]
    assert prepared.message.labels["trace_id"] == "abc"

    await prepared.kiq()

    assert broker.sent[0][0].task_id == prepared.message.task_id


async def test_prepared_invocation_keeps_route_snapshot() -> None:
    router = TaskiqRouter()
    first = RecordingBroker(router=router, broker_name="first")
    second = RecordingBroker(router=router, broker_name="second")
    first_flow = Flow("first")
    second_flow = Flow("second")

    @first.task(task_name="demo.task")
    async def demo_task(value: int) -> None:
        return None

    first_route = router.route_task(demo_task, broker=first, flow=first_flow)
    second_route = router.resolve_route(demo_task, broker=second, flow=second_flow)
    kicker = demo_task.kicker().with_route(first_route)

    prepared = kicker.prepare(1)
    kicker.with_route(second_route)

    await prepared.kiq()

    assert first.sent[0][1] == first_flow
    assert second.sent == []


async def test_prepared_invocation_resolves_default_route_snapshot() -> None:
    router = TaskiqRouter()
    source = RecordingBroker(router=router, broker_name="source")
    first = RecordingBroker(router=router, broker_name="first")
    second = RecordingBroker(router=router, broker_name="second")
    first_flow = Flow("first")
    second_flow = Flow("second")

    @source.task(task_name="demo.task")
    async def demo_task(value: int) -> None:
        return None

    router.route_task(demo_task, broker=first, flow=first_flow)
    prepared = demo_task.kicker().prepare(1)

    router.route_task(demo_task, broker=second, flow=second_flow)

    await prepared.kiq()

    assert source.sent == []
    assert first.sent[0][1] == first_flow
    assert second.sent == []


async def test_prepared_invocation_snapshots_current_default_flow() -> None:
    first_flow = Flow("default.first")
    second_flow = Flow("default.second")
    broker = RecordingBroker(default_flow=first_flow)

    @broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    prepared = demo_task.kicker().prepare()
    broker.default_flow = second_flow

    await prepared.kiq()
    await demo_task.kiq()

    assert [flow for _, flow in broker.sent] == [first_flow, second_flow]


async def test_router_task_decorator_can_choose_broker_and_flow() -> None:
    router = TaskiqRouter()
    target = RecordingBroker(router=router, broker_name="target")
    flow = Flow("target-flow")

    @router.task("demo.task", broker=target, flow=flow)
    async def demo_task() -> None:
        return None

    await demo_task.kiq()

    assert target.sent[0][0].task_name == "demo.task"
    assert target.sent[0][1] == flow


async def test_routed_dispatch_uses_target_middleware_and_result_backend() -> None:
    router = TaskiqRouter()
    source = RecordingBroker(router=router, broker_name="source")
    target = RecordingBroker(router=router, broker_name="target")
    source_events: list[tuple[str, str, str]] = []
    target_events: list[tuple[str, str, str]] = []
    target_backend = RecordingResultBackend()

    source.add_middlewares(RecordingMiddleware("source", source_events))
    target.add_middlewares(RecordingMiddleware("target", target_events))
    target.with_result_backend(target_backend)

    @source.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    router.route_task(demo_task, broker=target, flow=Flow("target"))

    sent_task = await demo_task.kiq()

    assert sent_task.result_backend is target_backend
    assert source_events == []
    assert target_events == [
        ("target", "pre_send", "demo.task"),
        ("target", "post_send", "demo.task"),
    ]


async def test_worker_lookup_uses_task_name_not_flow() -> None:
    broker = InMemoryBroker(await_inplace=True)
    calls: list[str] = []

    @broker.task(task_name="demo.first")
    async def first_task() -> str:
        calls.append("first")
        return "first"

    @broker.task(task_name="demo.second")
    async def second_task() -> str:
        calls.append("second")
        return "second"

    task = await first_task.kicker().with_flow(Flow(second_task.task_name)).kiq()
    result = await task.wait_result(timeout=2)

    assert result.return_value == "first"
    assert calls == ["first"]
