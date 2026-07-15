import asyncio
from collections.abc import AsyncGenerator, Callable, Sequence
from typing import Literal, cast

from taskiq.abc.broker import AckableMessage, AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import BrokerMessage, TaskiqMessage


class ReceiverLifecycleError(RuntimeError):
    """Marker error for listener and middleware lifecycle failures."""


class PrefetchCounterMiddleware(TaskiqMiddleware):
    """Track the observable number of messages in the prefetch queue."""

    def __init__(self) -> None:
        super().__init__()
        self.queued_messages = 0

    def on_prefetch_queue_add(self) -> None:
        """Record one queued delivery."""
        self.queued_messages += 1

    def on_prefetch_queue_remove(self) -> None:
        """Record one removed or discarded delivery."""
        self.queued_messages -= 1


class ObservedSemaphore(asyncio.Semaphore):
    """Semaphore that exposes when a Receiver starts waiting for capacity."""

    def __init__(self, value: int) -> None:
        super().__init__(value)
        self.acquire_started = asyncio.Event()
        self.acquire_attempts: asyncio.Queue[int] = asyncio.Queue()
        self._acquire_count = 0

    async def acquire(self) -> Literal[True]:
        """Record the wait before delegating to asyncio.Semaphore."""
        self._acquire_count += 1
        self.acquire_attempts.put_nowait(self._acquire_count)
        self.acquire_started.set()
        return await super().acquire()


class ControlledBroker(AsyncBroker):
    """Queue-backed broker with deterministic read and close barriers."""

    def __init__(self) -> None:
        super().__init__()
        self.incoming: asyncio.Queue[bytes | BaseException] = asyncio.Queue()
        self.read_started: asyncio.Queue[int] = asyncio.Queue()
        self.closed = asyncio.Event()
        self.listen_calls = 0
        self._read_count = 0

    async def kick(self, message: BrokerMessage) -> None:
        """Put one encoded message into the controlled listener queue."""
        await self.incoming.put(message.message)

    def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        """Create a new controlled listener."""
        self.listen_calls += 1
        return self._listen()

    async def _listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        try:
            while True:
                self._read_count += 1
                await self.read_started.put(self._read_count)
                item = await self.incoming.get()
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            self.closed.set()


class ListenerBroker(AsyncBroker):
    """Broker backed by a test-specific async-generator factory."""

    def __init__(
        self,
        listener_factory: Callable[
            [],
            AsyncGenerator[bytes | AckableMessage, None],
        ],
    ) -> None:
        super().__init__()
        self.listener_factory = listener_factory

    async def kick(self, message: BrokerMessage) -> None:
        """Ignore sends in listener-only tests."""

    def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        """Return a fresh listener from the configured factory."""
        return self.listener_factory()


def encoded_message(broker: AsyncBroker, task_name: str) -> bytes:
    """Build one valid transport payload for a registered task."""
    return broker.formatter.dumps(
        TaskiqMessage(
            task_id="receiver-listener-task",
            task_name=task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    ).message


def contains_exception(error: BaseException, expected: BaseException) -> bool:
    """Return whether an error or a cross-version exception group contains a cause."""
    if error is expected:
        return True
    nested = cast(
        Sequence[BaseException],
        getattr(error, "exceptions", ()),
    )
    return any(contains_exception(item, expected) for item in nested)


async def assert_semaphore_capacity(
    semaphore: asyncio.Semaphore,
    expected_capacity: int,
) -> None:
    """Assert exact available capacity using only semaphore operations."""
    acquired = 0
    try:
        for _ in range(expected_capacity):
            await asyncio.wait_for(semaphore.acquire(), timeout=1)
            acquired += 1
        assert semaphore.locked()
    finally:
        for _ in range(acquired):
            semaphore.release()
