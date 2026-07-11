import pytest

from taskiq import (
    Flow,
    FlowIdentity,
    FlowProtocol,
    TaskiqRouter,
    get_flow_identity,
)
from taskiq.warnings import TaskiqDeprecationWarning
from tests.routing.models import BrokerQueue
from tests.utils import RecordingBroker


def test_flow_identity_uses_logical_name() -> None:
    generic_flow = Flow("events").with_options(durable=True)
    broker_flow = BrokerQueue(name="events", durable=True)

    assert generic_flow.identity == FlowIdentity("events")
    assert get_flow_identity(broker_flow) == FlowIdentity("events")
    assert generic_flow.broker_options() == {"durable": True}


def test_route_task_does_not_register_subscription_by_default() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")
    flow = Flow("events")

    route = router.route_task("demo.task", broker=broker, flow=flow)

    assert route.flow == flow
    assert router.get_subscriptions(broker) == ()
    assert broker.get_subscribed_flows() == ()


def test_subscription_snapshot_and_explicit_unsubscribe() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")
    first_flow = Flow("events.first")
    second_flow = Flow("events.second")

    first = router.subscribe(broker, first_flow, "first.task")
    snapshot = router.subscriptions
    second = router.subscribe(broker, second_flow, "second.task")

    assert snapshot == (first,)
    assert router.subscriptions == (first, second)
    assert first.broker_name == "broker"
    assert router.unsubscribe(broker, first_flow) == first
    assert router.subscriptions == (second,)
    assert router.unsubscribe(broker, first_flow) is None


def test_unsubscribe_rejects_conflicting_flow_declaration() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")
    registered_flow = Flow("events").with_options(durable=True)
    conflicting_flow = Flow("events").with_options(durable=False)

    subscription = router.subscribe(broker, registered_flow)

    with pytest.raises(ValueError, match="different broker options"):
        router.unsubscribe(broker, conflicting_flow)

    assert router.subscriptions == (subscription,)


def test_route_task_subscribe_true_is_deprecated_shim() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")
    flow = Flow("events")

    with pytest.warns(TaskiqDeprecationWarning, match="router.subscribe"):
        route = router.route_task(
            "demo.task",
            broker=broker,
            flow=flow,
            subscribe=True,
        )

    subscription = router.get_subscriptions(broker)[0]

    assert route.flow == flow
    assert subscription.broker is broker
    assert subscription.flow == flow
    assert subscription.task_names == frozenset({"demo.task"})
    assert broker.get_subscribed_flows() == (flow,)


def test_route_update_does_not_mutate_existing_subscriptions() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")
    first_flow = Flow("events.first")
    second_flow = Flow("events.second")

    router.route_task("demo.task", broker=broker, flow=first_flow)
    router.subscribe(broker, first_flow, "demo.task")
    router.route_task("demo.task", broker=broker, flow=second_flow)

    subscription = router.get_subscriptions(broker)[0]
    route = router.resolve_route("demo.task")

    assert route.flow == second_flow
    assert subscription.flow == first_flow
    assert subscription.task_names == frozenset({"demo.task"})
    assert broker.get_subscribed_flows() == (first_flow,)


def test_subscribe_dedupes_flow_and_merges_task_names() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")
    flow = Flow("events")

    router.subscribe(broker, flow, "first.task")
    subscription = router.subscribe(broker, flow, "second.task")

    assert router.get_subscriptions(broker) == (subscription,)
    assert subscription.task_names == frozenset({"first.task", "second.task"})
    assert broker.get_subscribed_flows() == (flow,)


def test_subscribe_merges_same_identity_with_compatible_options() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")
    generic_flow = Flow("events").with_options(durable=True)
    broker_flow = BrokerQueue(name="events", durable=True)

    router.subscribe(broker, generic_flow, "first.task")
    subscription = router.subscribe(broker, broker_flow, "second.task")

    assert router.get_subscriptions(broker) == (subscription,)
    assert subscription.flow is generic_flow
    assert subscription.task_names == frozenset({"first.task", "second.task"})
    assert broker.get_subscribed_flows() == (generic_flow,)


@pytest.mark.parametrize(
    "conflicting_flow",
    [
        pytest.param(
            Flow("events").with_options(durable=False),
            id="generic-flow",
        ),
        pytest.param(
            BrokerQueue(name="events", durable=False),
            id="broker-specific-flow",
        ),
    ],
)
def test_subscribe_rejects_same_identity_with_different_options(
    conflicting_flow: FlowProtocol,
) -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")

    router.subscribe(broker, Flow("events").with_options(durable=True), "first.task")

    with pytest.raises(ValueError, match="different broker options"):
        router.subscribe(
            broker,
            conflicting_flow,
            "second.task",
        )


def test_subscribe_rejects_default_flow_option_conflict() -> None:
    router = TaskiqRouter()
    default_flow = Flow("events").with_options(durable=True)
    broker = RecordingBroker(
        router=router,
        broker_name="broker",
        default_flow=default_flow,
    )

    with pytest.raises(ValueError, match="different broker options"):
        router.subscribe(
            broker,
            BrokerQueue(name="events", durable=False),
            "demo.task",
        )


def test_subscribe_preserves_distinct_default_flow() -> None:
    router = TaskiqRouter()
    default_flow = Flow("default")
    subscribed_flow = Flow("events")
    broker = RecordingBroker(
        router=router,
        broker_name="broker",
        default_flow=default_flow,
    )

    router.subscribe(broker, subscribed_flow, "demo.task")

    assert broker.get_subscribed_flows() == (default_flow, subscribed_flow)


def test_subscribe_allows_same_identity_on_different_brokers() -> None:
    router = TaskiqRouter()
    first_broker = RecordingBroker(router=router, broker_name="first")
    second_broker = RecordingBroker(router=router, broker_name="second")

    first_subscription = router.subscribe(
        first_broker,
        Flow("events").with_options(durable=True),
        "first.task",
    )
    second_subscription = router.subscribe(
        second_broker,
        BrokerQueue(name="events", durable=False),
        "second.task",
    )

    assert router.get_subscriptions(first_broker) == (first_subscription,)
    assert router.get_subscriptions(second_broker) == (second_subscription,)


def test_subscribed_flows_dedupes_default_flow_by_identity() -> None:
    router = TaskiqRouter()
    default_flow = Flow("events").with_options(durable=True)
    broker = RecordingBroker(
        router=router,
        broker_name="broker",
        default_flow=default_flow,
    )

    subscription = router.subscribe(
        broker,
        BrokerQueue(name="events", durable=True),
        "demo.task",
    )

    assert broker.get_subscribed_flows() == (subscription.flow,)
