from typing import Any

import pytest

from taskiq import InMemoryBroker, SkipSendError, TaskiqMessage, TaskiqMiddleware
from taskiq.kicker import AsyncKicker
from tests.utils import AsyncQueueBroker


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


async def test_skip_send_error_drops_task() -> None:
    """SkipSendError in pre_send drops the task and returns the given task_id."""
    calls = []

    class _BeforeMiddleware(TaskiqMiddleware):
        def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
            calls.append("before.pre_send")
            return message

        def post_send(self, message: TaskiqMessage) -> None:
            calls.append("before.post_send")

    class _SkipMiddleware(TaskiqMiddleware):
        def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
            raise SkipSendError(task_id="winner")

    class _AfterMiddleware(TaskiqMiddleware):
        def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
            calls.append("after.pre_send")
            return message

    broker = AsyncQueueBroker().with_middlewares(
        _BeforeMiddleware(),
        _SkipMiddleware(),
        _AfterMiddleware(),
    )

    @broker.task
    async def run_task() -> None:
        pass

    task = await run_task.kiq()

    assert task.task_id == "winner"
    assert broker.queue.empty()
    assert calls == ["before.pre_send"]


async def test_other_pre_send_errors_propagate() -> None:
    """Only SkipSendError is swallowed, other pre_send errors still propagate."""

    class _FailingMiddleware(TaskiqMiddleware):
        def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
            raise ValueError("boom")

    broker = AsyncQueueBroker().with_middlewares(_FailingMiddleware())

    @broker.task
    async def run_task() -> None:
        pass

    with pytest.raises(ValueError, match="boom"):
        await run_task.kiq()
    assert broker.queue.empty()
