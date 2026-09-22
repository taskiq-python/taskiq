import asyncio
import contextvars
import os
import threading
from collections.abc import AsyncGenerator, Awaitable
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any

import anyio
from taskiq_dependencies import Depends

from taskiq.abc.broker import AckableMessage
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.receiver.receiver import Receiver, _PrefetchedMessage, _QueueSignal
from taskiq.state import TaskiqState
from tests.receiver.receiver_listener_support import (
    ReceiverLifecycleError,
    assert_semaphore_capacity,
)
from tests.utils import AsyncQueueBroker

process_resource_broker = AsyncQueueBroker()


@dataclass
class ProcessResourceProbe:
    """Share a resource path and explicit checkpoints with a spawned worker."""

    resource: Path
    started: threading.Event
    release: threading.Event


async def process_resource_dependency(
    state: TaskiqState = Depends(),
) -> AsyncGenerator[ProcessResourceProbe, None]:
    """Keep the resource available until the receiver closes the dependency."""
    probe: ProcessResourceProbe = state.process_resource
    try:
        yield probe
    finally:
        probe.resource.unlink()


@process_resource_broker.task
def read_process_resource(
    probe: ProcessResourceProbe = Depends(process_resource_dependency),
) -> tuple[int, str]:
    """Read the resource in an importable, spawn-compatible task function."""
    probe.started.set()
    if not probe.release.wait(timeout=15):
        raise TimeoutError("Process resource probe was not released")
    return os.getpid(), probe.resource.read_text()


class SyncResourceProbe:
    """Keep a real executor function alive while its dependency is inspected."""

    def __init__(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.started = asyncio.Event()
        self.finished = asyncio.Event()
        self.release = threading.Event()
        self.resource = StringIO("completed")
        self.continuation_started = asyncio.Event()
        self.continuation_finished = asyncio.Event()
        self.release_continuation = asyncio.Event()

    async def dependency(self) -> AsyncGenerator[StringIO, None]:
        try:
            yield self.resource
        finally:
            self.resource.close()

    def run(self, resource: StringIO) -> str:
        self.loop.call_soon_threadsafe(self.started.set)
        try:
            assert self.release.wait(timeout=10)
            return resource.getvalue()
        finally:
            self.loop.call_soon_threadsafe(self.finished.set)

    async def continuation(self) -> str:
        self.continuation_started.set()
        try:
            await self.release_continuation.wait()
            return self.resource.getvalue()
        finally:
            assert not self.resource.closed
            self.continuation_finished.set()


class DependencyCleanupProbe:
    """Expose teardown checkpoints while enforcing the original task/context."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.finished = asyncio.Event()
        self.context = contextvars.ContextVar("dependency-cleanup", default=False)

    async def dependency(self) -> AsyncGenerator[None, None]:
        owner = asyncio.current_task()
        token = self.context.set(True)
        try:
            yield
        finally:
            self.started.set()
            await self.release.wait()
            for _ in range(20):
                await asyncio.sleep(0)
            assert asyncio.current_task() is owner
            self.context.reset(token)
            self.finished.set()


async def listen_in_scope(
    receiver: Receiver,
    finish_event: asyncio.Event,
    scope: anyio.CancelScope,
) -> None:
    """Expose listener cancellation through a real AnyIO scope."""
    with scope:
        await receiver.listen(finish_event)


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
