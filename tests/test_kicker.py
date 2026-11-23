from collections.abc import Set as AbstractSet
from typing import Any

import pytest

from taskiq import InMemoryBroker, SimpleRetryMiddleware
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.exceptions import SendTaskError
from taskiq.kicker import AsyncKicker


class _TaskOnlyLabelMiddleware(TaskiqMiddleware):
    """Middleware that declares a label as task-only (never goes on wire)."""

    @classmethod
    def task_only_labels(cls) -> AbstractSet[str]:
        return super().task_only_labels() | {"label_not_on_wire"}


async def test_task_only_labels_not_serialized() -> None:
    """Labels doesn't go on wire when corresponding middlewares are registered."""
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(),
        _TaskOnlyLabelMiddleware(),
    )

    @broker.task(
        types_of_exceptions=(ValueError, TypeError),
        label_not_on_wire="test_label_value",
    )
    async def run_task() -> None:
        pass

    kicker = run_task.kicker()
    message = kicker._prepare_message()

    assert "types_of_exceptions" not in message.labels
    assert "types_of_exceptions" not in (message.labels_types or {})
    assert "label_not_on_wire" not in message.labels
    assert "label_not_on_wire" not in (message.labels_types or {})


async def test_task_only_labels_still_local_on_task() -> None:
    """The registered task still keeps the real label values locally."""
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(),
        _TaskOnlyLabelMiddleware(),
    )

    @broker.task(
        types_of_exceptions=(ValueError, TypeError),
        label_not_on_wire="test_label_value",
    )
    async def run_task() -> None:
        pass

    task = broker.find_task(run_task.task_name)
    assert task is not None
    assert task.labels["types_of_exceptions"] == (ValueError, TypeError)
    assert task.labels["label_not_on_wire"] == "test_label_value"


async def test_other_labels_still_serialized() -> None:
    """Unrelated labels are unaffected by the fix."""
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(),
        _TaskOnlyLabelMiddleware(),
    )

    kicker: AsyncKicker[Any, Any] = AsyncKicker(
        task_name="some_task",
        broker=broker,
        labels={"retries": 3, "queue": "high_priority"},
    )
    message = kicker._prepare_message()

    assert message.labels["retries"] == "3"
    assert message.labels["queue"] == "high_priority"


async def test_send_task_error_includes_broker_failure() -> None:
    """SendTaskError message names the underlying broker exception."""
    broker = InMemoryBroker()

    async def failing_kick(message: Any) -> None:
        raise ConnectionError("queue is unreachable")

    broker.kick = failing_kick  # type: ignore[method-assign]

    @broker.task
    async def run_task() -> None:
        pass

    with pytest.raises(SendTaskError) as exc_info:
        await run_task.kiq()

    assert "ConnectionError: queue is unreachable" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, ConnectionError)
