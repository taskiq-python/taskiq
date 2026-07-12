import asyncio
from typing import Literal

import pytest

from taskiq import Flow, TaskiqRoute
from taskiq.abc.formatter import TaskiqFormatter
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.exceptions import SendTaskError
from taskiq.message import BrokerMessage, TaskiqMessage
from tests.utils import RecordingBroker


class LifecycleError(Exception):
    """Marker error for send lifecycle tests."""


class LifecycleMiddleware(TaskiqMiddleware):
    """Record send hooks and optionally fail one phase."""

    def __init__(
        self,
        events: list[str],
        *,
        error: LifecycleError | None = None,
        fail_at: Literal["pre_send", "post_send"] | None = None,
    ) -> None:
        super().__init__()
        self.events = events
        self.error = error
        self.fail_at = fail_at

    def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        """Record and optionally fail before transport dispatch."""
        self.events.append("pre_send")
        if self.fail_at == "pre_send" and self.error is not None:
            raise self.error
        return message

    def post_send(self, message: TaskiqMessage) -> None:
        """Record and optionally fail after transport dispatch."""
        self.events.append("post_send")
        if self.fail_at == "post_send" and self.error is not None:
            raise self.error


class FailingFormatter(TaskiqFormatter):
    """Formatter that raises a configured error on serialization."""

    def __init__(self, error: LifecycleError) -> None:
        self.error = error

    def dumps(self, message: TaskiqMessage) -> BrokerMessage:
        """Fail message serialization."""
        raise self.error

    def loads(self, message: bytes) -> TaskiqMessage:
        """Incoming deserialization is not used by these tests."""
        raise AssertionError("loads() must not be called")


class AsyncOrderedMiddleware(TaskiqMiddleware):
    """Record async send hooks with a middleware name."""

    def __init__(self, name: str, events: list[str]) -> None:
        super().__init__()
        self.name = name
        self.events = events

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        """Record async pre-send order."""
        await asyncio.sleep(0)
        self.events.append(f"{self.name}.pre_send")
        return message

    async def post_send(self, message: TaskiqMessage) -> None:
        """Record async post-send order."""
        await asyncio.sleep(0)
        self.events.append(f"{self.name}.post_send")


class FailingSendBroker(RecordingBroker):
    """Broker that records transport entry and then fails."""

    def __init__(self, events: list[str], error: BaseException) -> None:
        self.events = events
        self.send_error = error
        super().__init__()

    async def kick(self, message: BrokerMessage) -> None:
        """Fail old-style transport dispatch."""
        self.events.append("kick")
        raise self.send_error

    async def kick_to_flow(
        self,
        message: BrokerMessage,
        flow: object | None = None,
    ) -> None:
        """Fail flow-aware transport dispatch."""
        self.events.append("kick")
        raise self.send_error


async def test_send_middleware_order_supports_async_hooks() -> None:
    events: list[str] = []
    broker = RecordingBroker()
    broker.with_middlewares(
        AsyncOrderedMiddleware("first", events),
        AsyncOrderedMiddleware("second", events),
    )

    @broker.task(task_name="lifecycle.order")
    async def demo_task() -> None:
        return None

    await demo_task.kiq()

    assert events == [
        "first.pre_send",
        "second.pre_send",
        "second.post_send",
        "first.post_send",
    ]


async def test_pre_send_error_is_not_wrapped() -> None:
    events: list[str] = []
    error = LifecycleError("pre_send")
    broker = RecordingBroker()
    broker.with_middlewares(
        LifecycleMiddleware(events, error=error, fail_at="pre_send"),
    )

    @broker.task(task_name="lifecycle.pre_send")
    async def demo_task() -> None:
        return None

    with pytest.raises(LifecycleError) as exc_info:
        await demo_task.kiq()

    assert exc_info.value is error
    assert events == ["pre_send"]
    assert broker.sent == []


async def test_post_send_error_is_not_wrapped() -> None:
    events: list[str] = []
    error = LifecycleError("post_send")
    broker = RecordingBroker()
    broker.with_middlewares(
        LifecycleMiddleware(events, error=error, fail_at="post_send"),
    )

    @broker.task(task_name="lifecycle.post_send")
    async def demo_task() -> None:
        return None

    with pytest.raises(LifecycleError) as exc_info:
        await demo_task.kiq()

    assert exc_info.value is error
    assert events == ["pre_send", "post_send"]
    assert len(broker.sent) == 1


async def test_formatter_error_is_wrapped_and_skips_post_send() -> None:
    events: list[str] = []
    error = LifecycleError("formatter")
    broker = RecordingBroker()
    broker.with_middlewares(LifecycleMiddleware(events))
    broker.with_formatter(FailingFormatter(error))

    @broker.task(task_name="lifecycle.formatter")
    async def demo_task() -> None:
        return None

    with pytest.raises(SendTaskError) as exc_info:
        await demo_task.kiq()

    assert exc_info.value.__cause__ is error
    assert events == ["pre_send"]
    assert broker.sent == []


async def test_transport_error_is_wrapped_and_skips_post_send() -> None:
    events: list[str] = []
    error = LifecycleError("transport")
    broker = FailingSendBroker(events, error)
    broker.with_middlewares(LifecycleMiddleware(events))

    @broker.task(task_name="lifecycle.transport")
    async def demo_task() -> None:
        return None

    with pytest.raises(SendTaskError) as exc_info:
        await demo_task.kiq()

    assert exc_info.value.__cause__ is error
    assert events == ["pre_send", "kick"]


async def test_transport_cancellation_is_not_wrapped() -> None:
    events: list[str] = []
    cancellation = asyncio.CancelledError()
    broker = FailingSendBroker(events, cancellation)
    broker.with_middlewares(LifecycleMiddleware(events))

    @broker.task(task_name="lifecycle.cancel")
    async def demo_task() -> None:
        return None

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await demo_task.kiq()

    assert exc_info.value is cancellation
    assert events == ["pre_send", "kick"]


async def test_route_configuration_error_is_not_wrapped() -> None:
    source = RecordingBroker()
    other = RecordingBroker(router=source.router, broker_name="other")

    @source.task(task_name="lifecycle.route")
    async def demo_task() -> None:
        return None

    kicker = demo_task.kicker()
    message = kicker.prepare().message

    with pytest.raises(ValueError, match="either route or broker override"):
        await kicker.kiq_message(
            message,
            broker=other,
            route=TaskiqRoute(source),
        )


async def test_kicker_rejects_route_with_separate_flow_override() -> None:
    broker = RecordingBroker()

    @broker.task(task_name="lifecycle.route-flow")
    async def demo_task() -> None:
        return None

    kicker = demo_task.kicker()
    message = kicker.prepare().message

    with pytest.raises(ValueError, match="either route or flow override"):
        await kicker.kiq_message(
            message,
            route=TaskiqRoute(broker),
            flow=Flow("override"),
        )


async def test_legacy_fallback_keeps_middleware_error_unwrapped() -> None:
    events: list[str] = []
    error = LifecycleError("legacy.pre_send")
    broker = RecordingBroker()
    broker.with_middlewares(
        LifecycleMiddleware(events, error=error, fail_at="pre_send"),
    )

    @broker.task(task_name="lifecycle.legacy")
    async def demo_task() -> None:
        return None

    del broker.router

    with pytest.raises(LifecycleError) as exc_info:
        await demo_task.kiq()

    assert exc_info.value is error
    assert events == ["pre_send"]
    assert broker.sent == []
