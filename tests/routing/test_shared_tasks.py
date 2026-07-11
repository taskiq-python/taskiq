from typing import Any

import pytest

from taskiq import AsyncTaskiqDecoratedTask, Flow, task_builder
from taskiq.brokers.shared_broker import AsyncSharedBroker, SharedDecoratedTask
from tests.routing.models import RecordingMiddleware, TracingTask
from tests.utils import RecordingBroker


class StorageError(Exception):
    """Marker error for broker-owned registration storage tests."""


class FailingStorageBroker(RecordingBroker):
    """Broker whose registration storage fails before publishing a task."""

    def store_registered_task(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> None:
        """Fail broker-owned task storage."""
        raise StorageError("storage failed")


async def test_task_builder_can_be_registered_later() -> None:
    broker = RecordingBroker()

    @task_builder("shared.add", queue="shared")
    def add(left: int, right: int) -> int:
        return left + right

    assert await add.call(1, 2) == 3

    registered = broker.register_task(add)

    assert type(registered) is AsyncTaskiqDecoratedTask
    assert registered.task_name == "shared.add"
    assert registered.labels == {"queue": "shared"}
    assert broker.router.find_task("shared.add") is registered

    await registered.kiq(1, 2)

    assert broker.sent[0][0].task_name == "shared.add"


async def test_router_register_task_definition_binds_to_selected_broker() -> None:
    source = RecordingBroker(broker_name="source")
    target = RecordingBroker(router=source.router, broker_name="target")
    flow = Flow("target")

    @task_builder("shared.routed")
    async def shared_task() -> None:
        return None

    registered = source.router.register_task(shared_task, broker=target, flow=flow)

    await registered.kiq()

    assert source.sent == []
    assert target.find_task("shared.routed") is registered
    assert target.local_task_registry["shared.routed"] is registered
    assert target.sent[0][0].task_name == "shared.routed"
    assert target.sent[0][1] == flow


async def test_task_builder_can_use_custom_base_cls() -> None:
    broker = RecordingBroker()

    @task_builder("shared.traced", base_cls=TracingTask, queue="shared")
    async def traced(value: int) -> int:
        return value + 1

    registered = broker.register_task(traced)

    assert isinstance(registered, TracingTask)
    assert registered.tracing_name() == "shared.traced"
    assert registered.labels == {"queue": "shared"}
    assert await traced.call(1) == 2

    await registered.kiq(1)

    assert broker.sent[0][0].task_name == "shared.traced"


async def test_task_builder_custom_base_cls_uses_broker_middleware() -> None:
    broker = RecordingBroker()
    events: list[tuple[str, str, str]] = []
    broker.add_middlewares(RecordingMiddleware("broker", events))

    @task_builder("shared.traced.middleware", base_cls=TracingTask)
    async def traced(value: int) -> int:
        return value + 1

    registered = broker.register_task(traced)

    assert isinstance(registered, TracingTask)

    await registered.kiq(1)

    assert events == [
        ("broker", "pre_send", "shared.traced.middleware"),
        ("broker", "post_send", "shared.traced.middleware"),
    ]


def test_task_definition_default_flow_does_not_create_subscription() -> None:
    flow = Flow("shared.default")
    broker = RecordingBroker(default_flow=flow)

    @task_builder("shared.default")
    def shared_task() -> None:
        return None

    registered = broker.register_task(shared_task)
    route = broker.router.resolve_route(registered)

    assert route.flow == flow
    assert broker.router.get_subscriptions(broker) == ()
    assert broker.get_subscribed_flows() == (flow,)


def test_register_task_definition_rejects_overrides() -> None:
    broker = RecordingBroker()

    @task_builder("shared.add", queue="shared")
    def add(left: int, right: int) -> int:
        return left + right

    with pytest.raises(ValueError, match="TaskDefinition already defines"):
        broker.register_task(add, task_name="other.name")

    with pytest.raises(ValueError, match="TaskDefinition already defines"):
        broker.register_task(add, queue="other")


async def test_task_definition_uses_shared_broker_registration_contract() -> None:
    task_name = "shared.global.definition"
    broker = AsyncSharedBroker()
    target = RecordingBroker()
    broker.default_broker(target)
    broker.global_task_registry.pop(task_name, None)

    @task_builder(task_name)
    async def shared_task() -> None:
        return None

    try:
        registered = broker.register_task(shared_task)

        assert isinstance(registered, SharedDecoratedTask)
        assert broker.global_task_registry[task_name] is registered
        assert task_name not in broker.local_task_registry
        assert broker.router.find_task(task_name) is registered

        await registered.kiq()

        assert target.sent[0][0].task_name == task_name
    finally:
        broker.global_task_registry.pop(task_name, None)


def test_storage_failure_does_not_publish_partial_router_state() -> None:
    broker = FailingStorageBroker()

    @task_builder("shared.storage.failure")
    def shared_task() -> None:
        return None

    with pytest.raises(StorageError, match="storage failed"):
        broker.register_task(shared_task)

    assert broker.router.find_task(shared_task.task_name) is None
    assert not broker.router.has_route(shared_task.task_name)
    assert shared_task.task_name not in broker.local_task_registry


def test_repeated_task_definition_binding_keeps_first_registration() -> None:
    broker = RecordingBroker()

    @task_builder("shared.duplicate")
    def shared_task() -> None:
        return None

    registered = broker.register_task(shared_task)
    bound_function_name = shared_task.original_func.__name__

    with pytest.raises(ValueError, match="already registered"):
        broker.register_task(shared_task)

    assert shared_task.original_func.__name__ == bound_function_name
    assert broker.router.find_task(shared_task.task_name) is registered
    assert broker.local_task_registry[shared_task.task_name] is registered
