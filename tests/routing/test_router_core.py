import pytest

from taskiq import TaskiqRoute, TaskiqRouter, TaskiqSubscription, task_builder
from tests.routing.models import CountingRouter
from tests.utils import RecordingBroker


def test_broker_creates_default_router() -> None:
    broker = RecordingBroker()

    assert broker.router.brokers[broker.broker_name] is broker
    assert broker.router.default_broker_name == broker.broker_name


def test_public_router_models_keep_router_module() -> None:
    assert TaskiqRoute.__module__ == "taskiq.router"
    assert TaskiqSubscription.__module__ == "taskiq.router"


def test_router_rejects_duplicate_broker_names() -> None:
    router = TaskiqRouter()
    RecordingBroker(router=router, broker_name="broker")

    with pytest.raises(ValueError, match="already registered"):
        RecordingBroker(router=router, broker_name="broker")


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
