import inspect
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from multiprocessing import get_context
from typing import Any

import pytest
from pydantic import BaseModel

from taskiq import (
    AsyncTaskiqDecoratedTask,
    InMemoryBroker,
    TaskDefinition,
    task_builder,
)
from taskiq.labels import LabelType


class CustomBrokerTask(AsyncTaskiqDecoratedTask[Any, Any]):
    """Custom broker-level task class used by old decorator tests."""

    def broker_extension(self) -> str:
        """Return a marker exposed by the broker-specific task class."""
        return self.task_name


class ExplicitTask(AsyncTaskiqDecoratedTask[Any, Any]):
    """Task class explicitly selected by a shared task declaration."""


@dataclass(frozen=True, slots=True)
class Payload:
    """Payload dataclass used to check message argument preparation."""

    value: int


class User(BaseModel):
    """Payload model used to check pydantic argument preparation."""

    name: str


class CallableIncrement:
    """Callable object used to prove non-function shared task binding."""

    def __init__(self, increment: int) -> None:
        self.increment = increment

    def __call__(self, value: int) -> int:
        """Increment one value."""
        return value + self.increment


class CollidingCallable:
    """Callable whose application state overlaps with the task API."""

    def __init__(self, increment: int) -> None:
        self.broker = "source-broker"
        self.increment = increment
        self.kiq = "source-kiq"
        self.labels = {"source": "callable-object"}
        self.original_func = "source-original-func"
        self.return_type = str
        self.task_name = "source.task-name"

    def __call__(self, value: int) -> int:
        """Increment one value without exposing state as task metadata."""
        return value + self.increment


@task_builder("shared.process_pool")
def shared_process_pool_add(left: int, right: int) -> int:
    """Add values in a spawned worker process."""
    return left + right


def build_process_pool_task(
    task_name: str,
    increment: int,
) -> TaskDefinition[..., int]:
    """Build one shared closure from a reusable declaration factory."""

    @task_builder(task_name)
    def generated(value: int) -> int:
        return value + increment

    return generated


def build_stateful_task(
    task_name: str,
    captured: object,
    positional_default: Any = 1,
    keyword_default: Any = 1,
) -> TaskDefinition[..., int]:
    """Build declarations that share code but vary execution state."""

    @task_builder(task_name)
    def generated(
        value: int = positional_default,
        *,
        scale: int = keyword_default,
    ) -> int:
        return value * scale + int(bool(captured))

    return generated


def build_annotated_task(
    task_name: str,
    annotation: object,
) -> TaskDefinition[..., object]:
    """Build declarations whose validation contract varies independently."""

    def generated(value: object) -> object:
        return value

    generated.__annotations__ = {
        "return": annotation,
        "value": annotation,
    }
    return task_builder(task_name)(generated)


factory_process_pool_add_one = build_process_pool_task(
    "shared.factory_add_one",
    1,
)
factory_process_pool_add_two = build_process_pool_task(
    "shared.factory_add_two",
    2,
)


async def test_task_definition_call_executes_sync_and_async_functions() -> None:
    @task_builder("shared.add")
    def add(left: int, right: int) -> int:
        return left + right

    @task_builder("shared.multiply")
    async def multiply(left: int, right: int) -> int:
        return left * right

    assert add(1, 2) == 3
    assert await add.call(1, 2) == 3
    assert await multiply(2, 3) == 6
    assert await multiply.call(2, 3) == 6

    broker = InMemoryBroker()
    registered_add = broker.register_task(add)
    registered_multiply = broker.register_task(multiply)

    assert not inspect.iscoroutinefunction(registered_add.original_func)
    assert inspect.iscoroutinefunction(registered_multiply.original_func)


def test_bare_task_builder_uses_qualified_function_name() -> None:
    @task_builder
    def implicit_task(value: int) -> int:
        return value + 1

    assert implicit_task.task_name == f"{__name__}:implicit_task"
    assert implicit_task(1) == 2


async def test_callable_object_task_definition_can_be_bound_and_sent() -> None:
    definition = task_builder("shared.callable-object")(CallableIncrement(2))
    broker = InMemoryBroker(await_inplace=True)

    registered = broker.register_task(definition)
    sent_task = await registered.kiq(3)
    result = await sent_task.wait_result()
    await broker.shutdown()

    assert definition(3) == 5
    assert registered.return_type is int
    assert registered.__wrapped__ is definition.original_func
    assert result.return_value == 5


async def test_callable_object_state_cannot_replace_task_contract() -> None:
    source = CollidingCallable(2)
    definition = task_builder(
        "shared.callable-owned-state",
        queue="expected",
    )(source)
    broker = InMemoryBroker(await_inplace=True)

    registered = broker.register_task(definition)
    sent_task = await registered.kiq(3)
    result = await sent_task.wait_result()
    await broker.shutdown()

    assert registered.broker is broker
    assert registered.task_name == "shared.callable-owned-state"
    assert registered.labels == {"queue": "expected"}
    assert registered.return_type is int
    assert registered.__wrapped__ is source
    assert registered.original_func(3) == 5
    assert callable(registered.kiq)
    assert vars(registered)["increment"] == 2
    assert broker.local_task_registry[registered.task_name] is registered
    assert result.return_value == 5


def test_task_binding_preserves_non_conflicting_custom_metadata() -> None:
    def source(value: int) -> int:
        return value + 1

    vars(source)["custom_metadata"] = "preserved"
    definition = task_builder("shared.custom-metadata")(source)

    registered = InMemoryBroker().register_task(definition)

    assert vars(registered)["custom_metadata"] == "preserved"


def test_callable_object_binding_rejects_ambiguous_redeclaration() -> None:
    task_builder("shared.callable-collision")(CallableIncrement(2))

    with pytest.raises(
        ValueError,
        match="Factory-generated TaskDefinitions must use unique task names",
    ):
        task_builder("shared.callable-collision")(CallableIncrement(2))


def test_equivalent_immutable_factory_state_reuses_binding() -> None:
    first = build_stateful_task(
        "shared.equivalent-state",
        frozenset({1, 2}),
        positional_default=(1, 2),
        keyword_default=2,
    )
    second = build_stateful_task(
        "shared.equivalent-state",
        frozenset({1, 2}),
        positional_default=(1, 2),
        keyword_default=2,
    )

    first_registered = InMemoryBroker().register_task(first)
    second_registered = InMemoryBroker().register_task(second)

    assert first_registered.original_func is second_registered.original_func


@pytest.mark.parametrize(
    ("task_name", "first_state", "second_state"),
    [
        pytest.param(
            "shared.mutable-state",
            ([1], 1, 1),
            ([1], 1, 1),
            id="equal-mutable-closure",
        ),
        pytest.param(
            "shared.positional-state",
            (frozenset({1}), 1, 1),
            (frozenset({1}), 1.0, 1),
            id="different-positional-default-type",
        ),
        pytest.param(
            "shared.signed-zero-state",
            (frozenset({1}), -0.0, 1),
            (frozenset({1}), 0.0, 1),
            id="different-signed-zero-default",
        ),
        pytest.param(
            "shared.keyword-state",
            (frozenset({1}), 1, 1),
            (frozenset({1}), 1, 2),
            id="different-keyword-default",
        ),
        pytest.param(
            "shared.frozenset-state",
            (frozenset({1}), 1, 1),
            (frozenset({2}), 1, 1),
            id="different-frozen-closure",
        ),
        pytest.param(
            "shared.frozenset-type-state",
            (frozenset({1}), 1, 1),
            (frozenset({1.0}), 1, 1),
            id="equal-frozen-values-with-different-types",
        ),
    ],
)
def test_different_factory_execution_state_rejects_binding_reuse(
    task_name: str,
    first_state: tuple[object, Any, Any],
    second_state: tuple[object, Any, Any],
) -> None:
    build_stateful_task(task_name, *first_state)

    with pytest.raises(
        ValueError,
        match="Factory-generated TaskDefinitions must use unique task names",
    ):
        build_stateful_task(task_name, *second_state)


def test_different_factory_annotations_reject_binding_reuse() -> None:
    build_annotated_task("shared.annotation-state", int)

    with pytest.raises(
        ValueError,
        match="Factory-generated TaskDefinitions must use unique task names",
    ):
        build_annotated_task("shared.annotation-state", str)


def test_task_definition_preserves_metadata_across_broker_bindings() -> None:
    @task_builder("shared.metadata")
    def calculate(left: int, right: int = 2) -> int:
        """Calculate a shared result."""
        return left * right

    source = calculate.original_func
    source_metadata = (
        source.__name__,
        source.__qualname__,
        source.__module__,
        source.__doc__,
        inspect.signature(source),
    )

    first_registered = InMemoryBroker().register_task(calculate)
    second_registered = InMemoryBroker().register_task(calculate)

    assert calculate.__name__ == source.__name__
    assert calculate.__qualname__ == source.__qualname__
    assert calculate.__module__ == source.__module__
    assert calculate.__doc__ == source.__doc__
    assert calculate.__wrapped__ is source
    assert inspect.signature(calculate) == inspect.signature(source)
    assert (
        source.__name__,
        source.__qualname__,
        source.__module__,
        source.__doc__,
        inspect.signature(source),
    ) == source_metadata

    for registered in (first_registered, second_registered):
        assert registered.__name__ == source.__name__
        assert registered.__qualname__ == source.__qualname__
        assert registered.__module__ == source.__module__
        assert registered.__doc__ == source.__doc__
        assert registered.__wrapped__ is source
        assert inspect.signature(registered) == inspect.signature(source)

    assert first_registered.original_func is second_registered.original_func


def test_task_definition_binding_runs_in_spawned_process_pool() -> None:
    registered = InMemoryBroker().register_task(shared_process_pool_add)

    with ProcessPoolExecutor(
        max_workers=1,
        mp_context=get_context("spawn"),
    ) as executor:
        result = executor.submit(registered.original_func, 2, 3).result(timeout=10)

    assert result == 5


def test_factory_task_bindings_run_in_spawned_process_pool() -> None:
    first_registered = InMemoryBroker().register_task(factory_process_pool_add_one)
    second_registered = InMemoryBroker().register_task(factory_process_pool_add_two)

    assert first_registered.original_func is not second_registered.original_func
    assert (
        first_registered.original_func.__name__
        != second_registered.original_func.__name__
    )

    with ProcessPoolExecutor(
        max_workers=1,
        mp_context=get_context("spawn"),
    ) as executor:
        first_result = executor.submit(
            first_registered.original_func,
            10,
        ).result(timeout=10)
        second_result = executor.submit(
            second_registered.original_func,
            10,
        ).result(timeout=10)

    assert first_result == 11
    assert second_result == 12


def test_factory_task_bindings_reject_duplicate_task_names() -> None:
    build_process_pool_task("shared.factory_duplicate", 1)

    with pytest.raises(
        ValueError,
        match="Factory-generated TaskDefinitions must use unique task names",
    ):
        build_process_pool_task("shared.factory_duplicate", 2)


def test_task_definition_message_matches_registered_kicker_message() -> None:
    @task_builder(
        "shared.message",
        priority=7,
        enabled=True,
        payload=b"abc",
    )
    def process(payload: Payload, user: User) -> None:
        return None

    broker = InMemoryBroker()
    registered = broker.register_task(process)

    definition_message = process.message(
        "task-id",
        Payload(value=1),
        User(name="Ada"),
    )
    prepared_message = (
        registered.kicker()
        .with_task_id("task-id")
        .prepare(
            Payload(value=1),
            User(name="Ada"),
        )
        .message
    )

    assert definition_message == prepared_message
    assert definition_message.args == [{"value": 1}, {"name": "Ada"}]
    assert definition_message.labels == {
        "enabled": "True",
        "payload": "YWJj",
        "priority": "7",
    }
    assert definition_message.labels_types == {
        "enabled": LabelType.BOOL.value,
        "payload": LabelType.BYTES.value,
        "priority": LabelType.INT.value,
    }


def test_task_definition_message_rejects_dataclass_types() -> None:
    @task_builder("shared.message")
    def process(payload: Payload) -> None:
        return None

    with pytest.raises(ValueError, match="Cannot serialize types"):
        process.message("task-id", Payload)  # type: ignore[arg-type]


def test_shared_task_without_base_cls_uses_broker_task_class() -> None:
    broker = InMemoryBroker()
    broker.decorator_class = CustomBrokerTask

    @broker.task(task_name="old.decorator")
    async def old_task() -> None:
        return None

    @task_builder("shared.default")
    async def shared_task() -> None:
        return None

    registered = broker.register_task(shared_task)

    assert isinstance(old_task, CustomBrokerTask)
    assert isinstance(registered, CustomBrokerTask)
    assert registered.broker_extension() == "shared.default"


def test_explicit_base_cls_overrides_broker_task_class() -> None:
    broker = InMemoryBroker()
    broker.decorator_class = CustomBrokerTask

    @task_builder("shared.explicit", base_cls=ExplicitTask)
    async def shared_task() -> None:
        return None

    registered = broker.register_task(shared_task)

    assert isinstance(registered, ExplicitTask)
    assert not isinstance(registered, CustomBrokerTask)


@pytest.mark.parametrize(
    "base_cls",
    [
        pytest.param(object, id="non-task-type"),
        pytest.param(0, id="falsey-runtime-value"),
    ],
)
def test_invalid_task_definition_base_cls_fails_when_bound(base_cls: Any) -> None:
    @task_builder("shared.invalid", base_cls=base_cls)
    def invalid_task() -> None:
        return None

    broker = InMemoryBroker()

    with pytest.raises(
        TypeError,
        match="base_cls must be a subclass of AsyncTaskiqDecoratedTask",
    ):
        broker.register_task(invalid_task)
