from typing import Any

from taskiq import InMemoryBroker
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
