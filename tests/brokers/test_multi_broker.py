import asyncio
from collections.abc import AsyncGenerator

import pytest

from taskiq import MultiBroker
from taskiq.acks import AckableMessage
from taskiq.brokers.inmemory_broker import InmemoryResultBackend
from taskiq.events import TaskiqEvents
from taskiq.message import BrokerMessage
from taskiq.receiver import Receiver
from tests.utils import AsyncQueueBroker


def build_message(labels: dict[str, str] | None = None) -> BrokerMessage:
    """Build a minimal broker message for kick tests."""
    return BrokerMessage(
        task_id="task-id",
        task_name="tests:test_task",
        message=b"payload",
        labels=labels or {},
    )


def test_multi_broker_requires_non_empty_routing() -> None:
    with pytest.raises(ValueError, match="non-empty routing"):
        MultiBroker()

    with pytest.raises(ValueError, match="non-empty routing"):
        MultiBroker(routing={})


def test_multi_broker_validates_routed_broker_types() -> None:
    with pytest.raises(ValueError, match="AsyncBroker"):
        MultiBroker(routing={"fast": 42})  # type: ignore[dict-item]


def test_multi_broker_validates_default_routing_label() -> None:
    with pytest.raises(ValueError, match="no broker mapped"):
        MultiBroker(
            routing={"fast": AsyncQueueBroker()},
            default_routing_label="slow",
        )


def test_task_registration_on_inner_broker_raises() -> None:
    first = AsyncQueueBroker()
    MultiBroker(routing={"first": first})

    with pytest.raises(ValueError, match="MultiBroker itself"):

        @first.task
        async def inner_task() -> None: ...

    with pytest.raises(ValueError, match="MultiBroker itself"):
        first.task()


async def test_kick_routes_by_queue_name_label() -> None:
    first, second = AsyncQueueBroker(), AsyncQueueBroker()
    broker = MultiBroker(routing={"first": first, "second": second})

    await broker.kick(build_message(labels={"queue_name": "second"}))

    assert first.queue.qsize() == 0
    assert second.queue.qsize() == 1


async def test_kick_without_label_uses_default_routing_label() -> None:
    first, second = AsyncQueueBroker(), AsyncQueueBroker()
    broker = MultiBroker(
        routing={"first": first, "second": second},
        default_routing_label="second",
    )

    await broker.kick(build_message())

    assert first.queue.qsize() == 0
    assert second.queue.qsize() == 1


async def test_kick_without_label_and_default_raises() -> None:
    broker = MultiBroker(routing={"first": AsyncQueueBroker()})

    with pytest.raises(ValueError, match="default_routing_label"):
        await broker.kick(build_message())


async def test_kick_with_unknown_queue_name_raises() -> None:
    first, second = AsyncQueueBroker(), AsyncQueueBroker()
    broker = MultiBroker(routing={"first": first, "second": second})

    with pytest.raises(ValueError, match="no broker mapped for queue 'fast'"):
        await broker.kick(build_message(labels={"queue_name": "fast"}))


async def test_kick_uses_custom_routing_label() -> None:
    first, second = AsyncQueueBroker(), AsyncQueueBroker()
    broker = MultiBroker(
        routing={"first": first, "emails": second},
        default_routing_label="first",
        routing_label="topic",
    )

    await broker.kick(build_message(labels={"topic": "emails"}))

    assert first.queue.qsize() == 0
    assert second.queue.qsize() == 1


async def test_same_broker_under_two_labels_is_listened_once() -> None:
    class ListenCountingBroker(AsyncQueueBroker):
        def __init__(self) -> None:
            super().__init__()
            self.listen_calls = 0

        async def listen(self) -> AsyncGenerator[AckableMessage, None]:
            self.listen_calls += 1
            async for message in super().listen():
                yield message

    shared, other = ListenCountingBroker(), AsyncQueueBroker()
    broker = MultiBroker(
        routing={"a": shared, "b": shared, "other": other},
        default_routing_label="a",
    )
    await other.kick(build_message())

    iterator = broker.listen()
    await asyncio.wait_for(iterator.__anext__(), timeout=1)
    await iterator.aclose()

    assert shared.listen_calls == 1


async def test_listen_yields_messages_from_all_brokers() -> None:
    first, second = AsyncQueueBroker(), AsyncQueueBroker()
    broker = MultiBroker(routing={"first": first, "second": second})
    await first.kick(build_message())
    await second.kick(build_message())
    await second.kick(build_message())

    received = 0
    iterator = broker.listen()
    for _ in range(3):
        await asyncio.wait_for(iterator.__anext__(), timeout=1)
        received += 1
    await iterator.aclose()

    assert received == 3
    assert first.queue.qsize() == 0
    assert second.queue.qsize() == 0


async def test_listen_with_single_broker() -> None:
    first = AsyncQueueBroker()
    broker = MultiBroker(routing={"first": first})
    await first.kick(build_message())

    iterator = broker.listen()
    await asyncio.wait_for(iterator.__anext__(), timeout=1)
    await iterator.aclose()

    assert first.queue.qsize() == 0


def test_process_flags_propagate_to_inner_brokers() -> None:
    first, second = AsyncQueueBroker(), AsyncQueueBroker()
    broker = MultiBroker(routing={"first": first, "second": second})

    broker.is_worker_process = True
    assert first.is_worker_process is True
    assert second.is_worker_process is True

    broker.is_worker_process = False
    assert first.is_worker_process is False

    broker.is_scheduler_process = True
    assert first.is_scheduler_process is True
    assert second.is_scheduler_process is True


async def test_startup_delegates_to_inner_brokers() -> None:
    started: list[str] = []
    first, second = AsyncQueueBroker(), AsyncQueueBroker()
    first.add_event_handler(
        TaskiqEvents.WORKER_STARTUP,
        lambda _: started.append("first"),
    )
    second.add_event_handler(
        TaskiqEvents.WORKER_STARTUP,
        lambda _: started.append("second"),
    )
    broker = MultiBroker(routing={"first": first, "second": second})
    broker.add_event_handler(
        TaskiqEvents.WORKER_STARTUP,
        lambda _: started.append("composite"),
    )
    broker.is_worker_process = True

    await broker.startup()

    assert sorted(started) == ["composite", "first", "second"]


async def test_shutdown_delegates_to_inner_brokers() -> None:
    shut_down: list[str] = []
    first, second = AsyncQueueBroker(), AsyncQueueBroker()
    first.add_event_handler(
        TaskiqEvents.WORKER_SHUTDOWN,
        lambda _: shut_down.append("first"),
    )
    second.add_event_handler(
        TaskiqEvents.WORKER_SHUTDOWN,
        lambda _: shut_down.append("second"),
    )
    broker = MultiBroker(routing={"first": first, "second": second})
    broker.add_event_handler(
        TaskiqEvents.WORKER_SHUTDOWN,
        lambda _: shut_down.append("composite"),
    )
    broker.is_worker_process = True

    await broker.shutdown()

    assert sorted(shut_down) == ["composite", "first", "second"]


async def test_shutdown_tolerates_failing_inner_brokers() -> None:
    shut_down: list[str] = []

    class FailingBroker(AsyncQueueBroker):
        async def shutdown(self) -> None:
            raise RuntimeError("shutdown failed")

    failing = FailingBroker()
    ok_broker = AsyncQueueBroker()
    ok_broker.add_event_handler(
        TaskiqEvents.CLIENT_SHUTDOWN,
        lambda _: shut_down.append("ok"),
    )
    broker = MultiBroker(routing={"bad": failing, "ok": ok_broker})
    broker.add_event_handler(
        TaskiqEvents.CLIENT_SHUTDOWN,
        lambda _: shut_down.append("composite"),
    )

    await broker.shutdown()

    assert "ok" in shut_down
    assert "composite" in shut_down


async def test_find_task_finds_tasks_registered_on_composite() -> None:
    broker = MultiBroker(routing={"first": AsyncQueueBroker()})

    @broker.task
    async def composite_task() -> None: ...

    assert broker.find_task(composite_task.task_name) is composite_task


async def test_get_all_tasks_returns_tasks_registered_on_composite() -> None:
    broker = MultiBroker(routing={"first": AsyncQueueBroker()})

    @broker.task
    async def composite_task() -> None: ...

    assert broker.get_all_tasks()[composite_task.task_name] is composite_task


async def test_with_result_backend_propagates_to_inner_brokers() -> None:
    first, second = AsyncQueueBroker(), AsyncQueueBroker()
    broker = MultiBroker(routing={"first": first, "second": second})
    backend = InmemoryResultBackend[None]()

    broker.with_result_backend(backend)

    assert broker.result_backend is backend
    assert first.result_backend is backend
    assert second.result_backend is backend


async def test_receiver_processes_tasks_from_all_brokers() -> None:
    first, second = AsyncQueueBroker(), AsyncQueueBroker()
    broker = MultiBroker(
        routing={"first": first, "second": second},
        default_routing_label="first",
    )
    broker.with_result_backend(InmemoryResultBackend())
    executed: list[str] = []

    @broker.task(labels={"queue_name": "first"})
    async def task_one() -> str:
        executed.append("one")
        return "one"

    @broker.task(labels={"queue_name": "second"})
    async def task_two() -> str:
        executed.append("two")
        return "two"

    first_handle = await task_one.kiq()
    second_handle = await task_two.kiq()

    receiver = Receiver(broker=broker)
    finish_event = asyncio.Event()
    listener = asyncio.create_task(receiver.listen(finish_event))

    first_result = await first_handle.wait_result(timeout=5)
    second_result = await second_handle.wait_result(timeout=5)

    assert first_result.return_value == "one"
    assert second_result.return_value == "two"
    assert sorted(executed) == ["one", "two"]

    finish_event.set()
    await asyncio.wait_for(listener, timeout=5)
    await broker.shutdown()


class DyingBroker(AsyncQueueBroker):
    """Broker that yields N messages and then fails like a dead cluster."""

    def __init__(self, messages_before_death: int) -> None:
        super().__init__()
        self.messages_before_death = messages_before_death

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        while self.messages_before_death > 0:
            self.messages_before_death -= 1
            data = await self.queue.get()
            yield AckableMessage(data=data, ack=self.queue.task_done)
        raise RuntimeError("cluster is down")


async def test_failed_broker_does_not_stop_healthy_brokers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    dying = DyingBroker(messages_before_death=2)
    healthy = AsyncQueueBroker()
    broker = MultiBroker(routing={"dying": dying, "healthy": healthy})

    for _ in range(2):
        await dying.kick(build_message())
    for _ in range(3):
        await healthy.kick(build_message())

    received = 0
    iterator = broker.listen()
    for _ in range(5):
        await asyncio.wait_for(iterator.__anext__(), timeout=1)
        received += 1
    await iterator.aclose()

    assert received == 5
    assert "cluster is down" in caplog.text


async def test_listen_ends_when_all_inner_brokers_fail() -> None:
    broker = MultiBroker(
        routing={
            "first": DyingBroker(messages_before_death=0),
            "second": DyingBroker(messages_before_death=0),
        },
    )

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(broker.listen().__anext__(), timeout=1)
