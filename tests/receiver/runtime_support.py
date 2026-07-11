import asyncio
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import Any

from taskiq import AsyncBroker, BrokerMessage, TaskiqRouter
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.receiver import Receiver
from taskiq.receiver.runtime import (
    WorkerRuntime,
    WorkerRuntimeOptions,
    normalize_broker_source,
)


class RuntimeTestError(Exception):
    """Marker exception for worker runtime failures."""


class FatalRuntimeTestError(BaseException):
    """Marker fatal signal that runtime cleanup must not swallow."""


class RuntimeBroker(AsyncBroker):
    """Controllable broker used for multi-broker lifecycle tests."""

    def __init__(
        self,
        *,
        router: TaskiqRouter,
        broker_name: str,
        events: list[str],
        startup_error: BaseException | None = None,
        listen_error: BaseException | None = None,
        shutdown_error: BaseException | None = None,
    ) -> None:
        self.events = events
        self.startup_error = startup_error
        self.listen_error = listen_error
        self.shutdown_error = shutdown_error
        self.listen_started = asyncio.Event()
        self.release_listener = asyncio.Event()
        self.iterator_closed = asyncio.Event()
        self.started = False
        self.queue: asyncio.Queue[bytes] = asyncio.Queue()
        super().__init__(router=router, broker_name=broker_name)

    async def startup(self) -> None:
        """Record startup and optionally fail after core resources start."""
        self.events.append(
            f"{self.broker_name}.startup."
            f"{'worker' if self.is_worker_process else 'client'}",
        )
        await super().startup()
        self.started = True
        if self.startup_error is not None:
            raise self.startup_error

    async def shutdown(self) -> None:
        """Record shutdown and close core resources."""
        self.events.append(f"{self.broker_name}.shutdown")
        await super().shutdown()
        self.started = False
        if self.shutdown_error is not None:
            raise self.shutdown_error

    async def kick(self, message: BrokerMessage) -> None:
        """Queue an encoded message for this listener."""
        if not self.started:
            raise RuntimeTestError(f"{self.broker_name} kicked before startup")
        self.events.append(f"{self.broker_name}.kick")
        await self.queue.put(message.message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """Listen until cancelled or a configured failure is released."""
        self.events.append(f"{self.broker_name}.listen")
        self.listen_started.set()
        try:
            if self.listen_error is not None:
                await self.release_listener.wait()
                raise self.listen_error
            while True:
                yield await self.queue.get()
        finally:
            self.events.append(f"{self.broker_name}.iterator_closed")
            self.iterator_closed.set()


class PrefetchRecordingMiddleware(TaskiqMiddleware):
    """Record broker deliveries admitted into the shared prefetch budget."""

    def __init__(self, broker_name: str, prefetched: list[str]) -> None:
        super().__init__()
        self.broker_name = broker_name
        self.prefetched = prefetched

    def on_prefetch_queue_add(self) -> None:
        """Record one delivery accepted by a Receiver prefetch queue."""
        self.prefetched.append(self.broker_name)


def runtime_options(**overrides: Any) -> WorkerRuntimeOptions:
    """Build runtime options with focused per-test overrides."""
    options = WorkerRuntimeOptions(
        validate_params=True,
        max_async_tasks=10,
        max_async_tasks_jitter=0,
        max_prefetch=0,
        propagate_exceptions=True,
        ack_type=None,
        max_tasks_to_execute=None,
        wait_tasks_timeout=1,
        shutdown_timeout=1,
        receiver_kwargs={},
    )
    return replace(options, **overrides)


def build_runtime(
    brokers: object,
    executor: ThreadPoolExecutor,
    *,
    options: WorkerRuntimeOptions | None = None,
    receiver_type: type[Receiver] = Receiver,
) -> WorkerRuntime:
    """Normalize broker configuration and build the runtime under test."""
    return WorkerRuntime(
        selection=normalize_broker_source(brokers),
        receiver_type=receiver_type,
        executor=executor,
        options=options or runtime_options(),
    )
