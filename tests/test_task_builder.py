from dataclasses import dataclass
from typing import Any

import pytest
from pydantic import BaseModel

from taskiq import AsyncTaskiqDecoratedTask, InMemoryBroker, task_builder
from taskiq.labels import LabelType


class CustomBrokerTask(AsyncTaskiqDecoratedTask[Any, Any]):
    """Custom broker-level task class used by old decorator tests."""


@dataclass(frozen=True, slots=True)
class Payload:
    """Payload dataclass used to check message argument preparation."""

    value: int


class User(BaseModel):
    """Payload model used to check pydantic argument preparation."""

    name: str


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


def test_shared_task_without_base_cls_uses_native_task_class() -> None:
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
    assert type(registered) is AsyncTaskiqDecoratedTask


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
