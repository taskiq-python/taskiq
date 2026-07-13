import asyncio
from collections.abc import Awaitable
from typing import Any

import pytest

from taskiq.abc.broker import AckableMessage
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.receiver.receiver import QUEUE_DONE, Receiver


class ReceiverLifecycleError(RuntimeError):
    """Marker error for deterministic runner failures."""


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


class FailingRemoveMiddleware(TaskiqMiddleware):
    """Fail before the runner transfers capacity to a callback."""

    def on_prefetch_queue_remove(self) -> None:
        raise ReceiverLifecycleError("prefetch remove failed")


class FailingSecondRemoveMiddleware(TaskiqMiddleware):
    """Fail while one previously admitted callback is still active."""

    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def on_prefetch_queue_remove(self) -> None:
        self.calls += 1
        if self.calls == 2:
            raise ReceiverLifecycleError("second prefetch remove failed")


class BlockingRemoveMiddleware(TaskiqMiddleware):
    """Expose cancellation before callback ownership is transferred."""

    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()

    async def on_prefetch_queue_remove(self) -> None:
        self.started.set()
        await asyncio.Event().wait()


class ReceiverQueue(asyncio.Queue[bytes | AckableMessage]):
    """Expose runner reads and graceful-shutdown sentinel delivery."""

    def __init__(self) -> None:
        super().__init__()
        self.get_started = asyncio.Event()
        self.shutdown_received = asyncio.Event()

    async def get(self) -> bytes | AckableMessage:
        self.get_started.set()
        message = await super().get()
        if message is QUEUE_DONE:
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
        callback_tasks = set(self.callback_tasks) | set(self._active_tasks)
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
    await queue.put(b"payload")
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
    for _ in range(slots):
        await asyncio.wait_for(receiver.sem.acquire(), timeout=0.1)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(receiver.sem.acquire(), timeout=0.01)
    for _ in range(slots):
        receiver.sem.release()
