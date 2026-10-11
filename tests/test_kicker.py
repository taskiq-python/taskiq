from typing import Any

import pytest

from taskiq import InMemoryBroker
from taskiq.exceptions import SendTaskError
from taskiq.kicker import AsyncKicker


async def test_types_of_exceptions_not_serialized() -> None:
    """`types_of_exceptions` label should never be sent over the wire."""
    broker = InMemoryBroker()

    @broker.task(types_of_exceptions=(ValueError, TypeError))
    async def run_task() -> None:
        pass

    kicker = run_task.kicker()
    message = kicker._prepare_message()

    assert "types_of_exceptions" not in message.labels
    assert "types_of_exceptions" not in (message.labels_types or {})


async def test_types_of_exceptions_still_local_on_task() -> None:
    """The registered task still keeps the real exception types locally."""
    broker = InMemoryBroker()

    @broker.task(types_of_exceptions=(ValueError, TypeError))
    async def run_task() -> None:
        pass

    task = broker.find_task(run_task.task_name)
    assert task is not None
    assert task.labels["types_of_exceptions"] == (ValueError, TypeError)


async def test_other_labels_still_serialized() -> None:
    """Unrelated labels are unaffected by the fix."""
    kicker: AsyncKicker[Any, Any] = AsyncKicker(
        task_name="some_task",
        broker=InMemoryBroker(),
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
