import pytest

from taskiq import TaskiqRoute, TaskiqRouter, TaskiqSubscription, task_builder
from tests.routing.models import CountingRouter
from tests.utils import RecordingBroker


def test_broker_creates_default_router() -> None:
    broker = RecordingBroker()

    assert broker.router.brokers[broker.broker_name] is broker
    assert broker.router.default_broker is broker
    assert broker.router.default_broker_name == broker.broker_name


def test_public_router_models_keep_router_module() -> None:
    assert TaskiqRoute.__module__ == "taskiq.router"
    assert TaskiqSubscription.__module__ == "taskiq.router"


def test_router_rejects_duplicate_broker_names() -> None:
    router = TaskiqRouter()
    RecordingBroker(router=router, broker_name="broker")

    with pytest.raises(ValueError, match="already registered"):
        RecordingBroker(router=router, broker_name="broker")


def test_router_rejects_registering_same_broker_under_another_name() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")

    with pytest.raises(ValueError, match="already registered as 'broker'"):
        router.set_broker(broker, name="alias")


def test_router_rejects_unknown_broker_lookup() -> None:
    router = TaskiqRouter()

    with pytest.raises(ValueError, match="Unknown broker 'missing'"):
        router.get_broker("missing")


def test_router_can_clear_default_broker_explicitly() -> None:
    router = TaskiqRouter()
    RecordingBroker(router=router, broker_name="broker")

    router.default_broker = None

    assert router.default_broker is None
    assert router.default_broker_name is None
    with pytest.raises(ValueError, match="doesn't have registered brokers"):
        router.resolve_route("demo.task")


def test_router_rejects_broker_attached_to_another_router() -> None:
    first_router = TaskiqRouter()
    second_router = TaskiqRouter()
    broker = RecordingBroker(router=first_router, broker_name="broker")

    with pytest.raises(ValueError, match="attached to another router"):
        second_router.set_broker(broker, name="broker")


def test_router_default_broker_setter_rejects_foreign_broker() -> None:
    first_router = TaskiqRouter()
    second_router = TaskiqRouter()
    broker = RecordingBroker(router=first_router, broker_name="broker")

    with pytest.raises(ValueError, match="not registered"):
        second_router.default_broker = broker


def test_router_rejects_string_broker_references() -> None:
    router = TaskiqRouter()
    RecordingBroker(router=router, broker_name="broker")

    with pytest.raises(TypeError, match="Broker string references"):
        router.route_task("demo.task", broker="broker")  # type: ignore[arg-type]


def test_router_rejects_invalid_task_reference() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")

    with pytest.raises(TypeError, match="task name or decorated task"):
        router.route_task(object(), broker=broker)  # type: ignore[arg-type]


def test_router_routes_are_immutable_resolved_snapshots() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")
    route = router.route_task("demo.task", broker=broker)

    routes = router.routes

    assert routes == {"demo.task": route}
    with pytest.raises(TypeError):
        routes["other.task"] = route  # type: ignore[index]

    assert router.has_route("demo.task")
    assert router.remove_route("demo.task") == route
    assert not router.has_route("demo.task")
    assert router.remove_route("demo.task") is None


def test_broker_and_task_registries_are_immutable_snapshots() -> None:
    router = TaskiqRouter()
    first = RecordingBroker(router=router, broker_name="first")
    broker_snapshot = router.brokers
    task_snapshot = router.task_registry

    @first.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    second = RecordingBroker(router=router, broker_name="second")

    assert broker_snapshot == {"first": first}
    assert task_snapshot == {}
    assert router.brokers == {"first": first, "second": second}
    assert router.task_registry == {"demo.task": demo_task}

    with pytest.raises(TypeError):
        broker_snapshot["other"] = first  # type: ignore[index]
    with pytest.raises(TypeError):
        task_snapshot["other.task"] = demo_task  # type: ignore[index]


def test_router_rejects_duplicate_task_names() -> None:
    router = TaskiqRouter()
    first = RecordingBroker(router=router, broker_name="first")
    second = RecordingBroker(router=router, broker_name="second")

    @first.task(task_name="demo.task")
    async def first_task() -> None:
        return None

    with pytest.raises(ValueError, match="already registered"):

        @second.task(task_name="demo.task")
        async def second_task() -> None:
            return None


def test_task_definition_binding_does_not_reenter_router_registration() -> None:
    router = CountingRouter()
    broker = RecordingBroker(router=router)

    @task_builder("shared.once")
    def shared_task() -> None:
        return None

    registered = broker.register_task(shared_task)

    assert registered.task_name == "shared.once"
    assert router.register_task_calls == 1
    assert broker.router.find_task("shared.once") is registered
    assert broker.find_task("shared.once") is registered


def test_router_infers_broker_from_prebound_task() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="broker")

    @task_builder("shared.prebound")
    def shared_task() -> None:
        return None

    bound_task = broker.bind_task_definition(shared_task, register=False)

    registered = router.register_task(bound_task)

    assert registered is bound_task
    assert router.resolve_route(registered).broker is broker


def test_bound_task_registration_validates_route_before_registry_mutation() -> None:
    router = TaskiqRouter()
    broker = RecordingBroker(router=router, broker_name="local")
    foreign_broker = RecordingBroker(broker_name="foreign")

    @task_builder("shared.invalid-route")
    def shared_task() -> None:
        return None

    bound_task = broker.bind_task_definition(shared_task, register=False)

    with pytest.raises(ValueError, match="not registered in this router"):
        router.register_task(bound_task, broker=foreign_broker)

    assert router.find_task(bound_task.task_name) is None
    assert not router.has_route(bound_task.task_name)
