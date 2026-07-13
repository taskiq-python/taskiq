import asyncio
from collections.abc import Awaitable
from typing import Any

from taskiq.abc.broker import AckableMessage
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.receiver.receiver import Receiver, _PrefetchedMessage, _QueueSignal
from tests.receiver.receiver_listener_support import (
    ReceiverLifecycleError,
    assert_semaphore_capacity,
)


class ShieldCallCounter:
    """Count shield calls made by one named asyncio task."""

    def __init__(self, task_name: str) -> None:
        self.task_name = task_name
        self.runner_calls = 0
        self._shield = asyncio.shield

    def __call__(self, awaitable: Awaitable[Any]) -> asyncio.Future[Any]:
        current_task = asyncio.current_task()
        if current_task is not None and current_task.get_name() == self.task_name:
            self.runner_calls += 1
        return self._shield(awaitable)


class ReceiverQueue(asyncio.Queue[_PrefetchedMessage | _QueueSignal]):
    """Expose runner reads and graceful-shutdown sentinel delivery."""

    def __init__(self) -> None:
        super().__init__()
        self.get_started = asyncio.Event()
        self.shutdown_received = asyncio.Event()

    async def get(self) -> _PrefetchedMessage | _QueueSignal:
        self.get_started.set()
        message = await super().get()
        if message is _QueueSignal.DONE:
            self.shutdown_received.set()
        return message


class ControlledReceiver(Receiver):
    """Receiver with deterministic callback and cleanup checkpoints."""

    def __init__(
        self,
        *,
        wait_tasks_timeout: float | None,
        max_async_tasks: int | None = 1,
        fail: bool = False,
    ) -> None:
        super().__init__(
            InMemoryBroker(),
            max_async_tasks=max_async_tasks,
            run_startup=False,
            wait_tasks_timeout=wait_tasks_timeout,
        )
        self.fail = fail
        self.started_callbacks: asyncio.Queue[None] = asyncio.Queue()
        self.release_callback = asyncio.Event()
        self.cleanup_started_callbacks: asyncio.Queue[None] = asyncio.Queue()
        self.release_cleanup = asyncio.Event()
        self.finished_callbacks: asyncio.Queue[None] = asyncio.Queue()
        self.callback_tasks: list[asyncio.Task[None]] = []

    @property
    def callback_task(self) -> asyncio.Task[None] | None:
        """Return the most recently started callback task."""
        if not self.callback_tasks:
            return None
        return self.callback_tasks[-1]

    async def callback(
        self,
        message: bytes | AckableMessage,
        raise_err: bool = False,
    ) -> None:
        del message, raise_err
        callback_task = asyncio.current_task()
        assert callback_task is not None
        self.callback_tasks.append(callback_task)
        self.started_callbacks.put_nowait(None)
        try:
            await self.release_callback.wait()
            if self.fail:
                raise ReceiverLifecycleError("callback failed")
        finally:
            self.cleanup_started_callbacks.put_nowait(None)
            await self.release_cleanup.wait()
            for _ in range(20):
                await asyncio.sleep(0)
            self.finished_callbacks.put_nowait(None)

    async def settle(self, runner_task: asyncio.Task[None]) -> None:
        """Release all checkpoints and settle test-owned tasks."""
        self.release_callback.set()
        self.release_cleanup.set()
        callback_tasks = set(self.callback_tasks)
        for callback_task in callback_tasks:
            if not callback_task.done():
                callback_task.cancel()
        if not runner_task.done():
            runner_task.cancel()
        await asyncio.gather(
            *callback_tasks,
            runner_task,
            return_exceptions=True,
        )


async def start_callback(
    receiver: ControlledReceiver,
) -> tuple[ReceiverQueue, asyncio.Task[None]]:
    """Start one controlled callback through the real runner boundary."""
    queue = ReceiverQueue()
    await queue.put(_PrefetchedMessage(b"payload", owns_delivery_slot=False))
    runner_task = asyncio.create_task(receiver.runner(queue))
    await wait_for_signals(receiver.started_callbacks)
    return queue, runner_task


async def wait_for_signals(
    signals: asyncio.Queue[None],
    count: int = 1,
) -> None:
    """Wait for an exact number of deterministic lifecycle checkpoints."""
    await asyncio.wait_for(
        asyncio.gather(*(signals.get() for _ in range(count))),
        timeout=1,
    )


async def assert_exact_capacity(receiver: Receiver, slots: int = 1) -> None:
    """Assert that exactly the expected execution permits were returned."""
    assert receiver.sem is not None
    await assert_semaphore_capacity(receiver.sem, slots)
