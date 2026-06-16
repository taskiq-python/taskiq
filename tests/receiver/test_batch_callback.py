import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from taskiq import InMemoryBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.acks import AckableMessage, AcknowledgeType
from taskiq.message import TaskiqMessage
from taskiq.receiver import Receiver
from taskiq.receiver.receiver import QUEUE_DONE
from taskiq.result import TaskiqResult


def _msg(broker: InMemoryBroker, name: str, item: int) -> bytes:
    return broker.formatter.dumps(
        TaskiqMessage(
            task_id=f"id-{item}",
            task_name=name,
            labels={},
            args=[item],
            kwargs={},
        ),
    ).message


def _receiver(
    broker: InMemoryBroker,
    ack_type: AcknowledgeType | None = None,
    max_async_tasks: int = 10,
) -> Receiver:
    return Receiver(
        broker,
        executor=ThreadPoolExecutor(max_workers=4),
        max_async_tasks=max_async_tasks,
        ack_type=ack_type,
    )


async def test_batch_callback_collects_items() -> None:
    broker = InMemoryBroker()
    seen: list[list[int]] = []

    @broker.task(batch=True, batch_size=3)
    async def my_task(items: list[int]) -> int:
        seen.append(items)
        return sum(items)

    receiver = _receiver(broker)
    msgs = [_msg(broker, my_task.task_name, i) for i in range(3)]
    await receiver.batched_callback(my_task.task_name, msgs)
    assert seen == [[0, 1, 2]]


async def test_batch_result_stored_for_each_task_id() -> None:
    broker = InMemoryBroker()

    @broker.task(batch=True, batch_size=2)
    async def my_task(items: list[int]) -> int:
        return sum(items)

    receiver = _receiver(broker)
    msgs = [_msg(broker, my_task.task_name, i) for i in (10, 20)]
    await receiver.batched_callback(my_task.task_name, msgs)

    r0 = await broker.result_backend.get_result("id-10")
    r1 = await broker.result_backend.get_result("id-20")
    assert r0.return_value == 30
    assert r1.return_value == 30


async def test_batch_acks_each_message() -> None:
    broker = InMemoryBroker()
    acked: list[int] = []

    @broker.task(batch=True, batch_size=2)
    async def my_task(items: list[int]) -> int:
        return sum(items)

    receiver = _receiver(broker)

    def make_ack(i: int) -> Callable[[], None]:
        def _ack() -> None:
            acked.append(i)

        return _ack

    msgs = [
        AckableMessage(data=_msg(broker, my_task.task_name, i), ack=make_ack(i))
        for i in (1, 2)
    ]
    await receiver.batched_callback(my_task.task_name, msgs)
    assert sorted(acked) == [1, 2]


async def test_batch_error_marks_all() -> None:
    broker = InMemoryBroker()

    @broker.task(batch=True, batch_size=2)
    async def my_task(items: list[int]) -> int:
        raise ValueError("boom")

    receiver = _receiver(broker)
    msgs = [_msg(broker, my_task.task_name, i) for i in (1, 2)]
    await receiver.batched_callback(my_task.task_name, msgs)

    r0 = await broker.result_backend.get_result("id-1")
    r1 = await broker.result_backend.get_result("id-2")
    assert r0.is_err
    assert r1.is_err


async def test_batch_unknown_task_is_safe() -> None:
    broker = InMemoryBroker()
    receiver = _receiver(broker)
    # Should not raise even though there are no messages / unknown task.
    await receiver.batched_callback("does.not.exist", [])


async def test_runner_routes_and_flushes_on_shutdown() -> None:
    broker = InMemoryBroker()
    seen: list[list[int]] = []

    @broker.task(batch=True, batch_size=5, batch_timeout=0.05)
    async def my_task(items: list[int]) -> int:
        seen.append(items)
        return sum(items)

    receiver = _receiver(broker)

    queue: asyncio.Queue[bytes | AckableMessage] = asyncio.Queue()
    for i in (1, 2):
        await queue.put(_msg(broker, my_task.task_name, i))
    await queue.put(QUEUE_DONE)

    await receiver.runner(queue)
    await asyncio.sleep(0.1)

    assert seen == [[1, 2]]


async def test_runner_flushes_on_size_during_run() -> None:
    broker = InMemoryBroker()
    seen: list[list[int]] = []

    @broker.task(batch=True, batch_size=2)
    async def my_task(items: list[int]) -> int:
        seen.append(items)
        return sum(items)

    receiver = _receiver(broker)

    queue: asyncio.Queue[bytes | AckableMessage] = asyncio.Queue()
    for i in (1, 2):
        await queue.put(_msg(broker, my_task.task_name, i))
    await queue.put(QUEUE_DONE)

    await receiver.runner(queue)
    await asyncio.sleep(0.05)

    assert seen == [[1, 2]]


async def test_runner_still_runs_normal_tasks() -> None:
    broker = InMemoryBroker()
    calls: list[int] = []

    @broker.task
    async def normal_task(x: int) -> int:
        calls.append(x)
        return x

    receiver = _receiver(broker)

    queue: asyncio.Queue[bytes | AckableMessage] = asyncio.Queue()
    await queue.put(_msg(broker, normal_task.task_name, 7))
    await queue.put(QUEUE_DONE)

    await receiver.runner(queue)
    await asyncio.sleep(0.05)

    assert calls == [7]


async def test_batch_on_error_runs_per_message() -> None:
    """on_error must fire once per message in the batch, not once per batch."""
    errored: list[str] = []

    class _ErrMiddleware(TaskiqMiddleware):
        def on_error(
            self,
            message: TaskiqMessage,
            result: "TaskiqResult[Any]",
            exception: BaseException,
        ) -> None:
            errored.append(message.task_id)

    broker = InMemoryBroker().with_middlewares(_ErrMiddleware())

    @broker.task(batch=True, batch_size=3)
    async def my_task(items: list[int]) -> int:
        raise ValueError("boom")

    receiver = _receiver(broker)
    msgs = [_msg(broker, my_task.task_name, i) for i in range(3)]
    await receiver.batched_callback(my_task.task_name, msgs)

    assert sorted(errored) == ["id-0", "id-1", "id-2"]


async def test_batch_pre_execute_transformation_is_applied() -> None:
    """A pre_execute middleware that returns a new message must take effect."""
    seen: list[list[int]] = []

    class _TransformMiddleware(TaskiqMiddleware):
        def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
            # Double the single item carried by each message.
            message.args = [message.args[0] * 2]
            return message

    broker = InMemoryBroker().with_middlewares(_TransformMiddleware())

    @broker.task(batch=True, batch_size=3)
    async def my_task(items: list[int]) -> int:
        seen.append(items)
        return sum(items)

    receiver = _receiver(broker)
    msgs = [_msg(broker, my_task.task_name, i) for i in (1, 2, 3)]
    await receiver.batched_callback(my_task.task_name, msgs)

    assert seen == [[2, 4, 6]]


async def test_batch_acks_when_received() -> None:
    broker = InMemoryBroker()
    acked: list[int] = []

    @broker.task(batch=True, batch_size=2)
    async def my_task(items: list[int]) -> int:
        # Record ack order relative to execution.
        acked.append(-1)
        return sum(items)

    receiver = _receiver(broker, ack_type=AcknowledgeType.WHEN_RECEIVED)

    def make_ack(i: int) -> Callable[[], None]:
        def _ack() -> None:
            acked.append(i)

        return _ack

    msgs = [
        AckableMessage(data=_msg(broker, my_task.task_name, i), ack=make_ack(i))
        for i in (1, 2)
    ]
    await receiver.batched_callback(my_task.task_name, msgs)

    # Both messages acked, and acks happened before execution.
    assert sorted(x for x in acked if x != -1) == [1, 2]
    assert acked.index(1) < acked.index(-1)
    assert acked.index(2) < acked.index(-1)


async def test_batch_acks_when_executed() -> None:
    broker = InMemoryBroker()
    acked: list[int] = []

    @broker.task(batch=True, batch_size=2)
    async def my_task(items: list[int]) -> int:
        acked.append(-1)
        return sum(items)

    receiver = _receiver(broker, ack_type=AcknowledgeType.WHEN_EXECUTED)

    def make_ack(i: int) -> Callable[[], None]:
        def _ack() -> None:
            acked.append(i)

        return _ack

    msgs = [
        AckableMessage(data=_msg(broker, my_task.task_name, i), ack=make_ack(i))
        for i in (1, 2)
    ]
    await receiver.batched_callback(my_task.task_name, msgs)

    assert sorted(x for x in acked if x != -1) == [1, 2]
    # Acks happened after execution.
    assert acked.index(-1) < acked.index(1)
    assert acked.index(-1) < acked.index(2)


async def test_buffering_not_blocked_by_saturated_semaphore() -> None:
    """Batched messages buffer freely even when the semaphore is exhausted."""
    broker = InMemoryBroker()
    started = asyncio.Event()
    release = asyncio.Event()
    seen: list[list[int]] = []

    @broker.task
    async def blocker(x: int) -> int:
        started.set()
        await release.wait()
        return x

    @broker.task(batch=True, batch_size=3)
    async def my_task(items: list[int]) -> int:
        seen.append(items)
        return sum(items)

    # Only one concurrent slot.
    receiver = _receiver(broker, max_async_tasks=1)

    queue: asyncio.Queue[bytes | AckableMessage] = asyncio.Queue()
    await queue.put(_msg(broker, blocker.task_name, 99))
    # These 3 batched messages must buffer and flush even though the single
    # semaphore slot is held by `blocker`.
    for i in range(3):
        await queue.put(_msg(broker, my_task.task_name, i))

    runner_task = asyncio.create_task(receiver.runner(queue))

    # Wait for the blocker to occupy the only slot.
    await asyncio.wait_for(started.wait(), timeout=2)
    # Give the runner time to drain the 3 batched messages into the buffer
    # and flush them (the batch waits for the slot at flush time).
    await asyncio.sleep(0.2)

    # Buffering happened (not blocked); the batch flush is now waiting for the
    # semaphore held by `blocker`. Release the blocker so everything finishes.
    release.set()
    await queue.put(QUEUE_DONE)
    await asyncio.wait_for(runner_task, timeout=2)

    assert seen == [[0, 1, 2]]
