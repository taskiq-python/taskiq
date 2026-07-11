import asyncio
import logging
from collections.abc import AsyncGenerator, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import Any

import pytest

from taskiq import AsyncBroker, BrokerMessage, TaskiqRouter, task_builder
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.receiver import Receiver
from taskiq.receiver.runtime import (
    ReceiverRuntimeState,
    WorkerRuntime,
    WorkerRuntimeConfigurationError,
    WorkerRuntimeOptions,
    normalize_broker_source,
)
from taskiq.result_backends.dummy import DummyResultBackend


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


@pytest.fixture
def runtime_executor() -> Iterator[ThreadPoolExecutor]:
    """Provide the process-wide sync executor owned by a worker runtime."""
    with ThreadPoolExecutor(max_workers=2) as executor:
        yield executor


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


async def test_runtime_listens_concurrently_and_starts_outbound_broker(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    outbound = RuntimeBroker(router=router, broker_name="outbound", events=events)
    finish_event = asyncio.Event()
    runtime = build_runtime((first, second), runtime_executor)

    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await asyncio.gather(first.listen_started.wait(), second.listen_started.wait())

    assert not runtime_task.done()
    assert not outbound.listen_started.is_set()
    assert first.is_worker_process
    assert second.is_worker_process
    assert not outbound.is_worker_process
    assert events[:3] == [
        "first.startup.worker",
        "second.startup.worker",
        "outbound.startup.client",
    ]

    finish_event.set()
    await runtime_task

    assert first.iterator_closed.is_set()
    assert second.iterator_closed.is_set()
    assert [event for event in events if event.endswith(".shutdown")] == [
        "outbound.shutdown",
        "second.shutdown",
        "first.shutdown",
    ]


async def test_single_broker_runtime_does_not_start_router_peers(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    listener = RuntimeBroker(router=router, broker_name="listener", events=events)
    peer = RuntimeBroker(router=router, broker_name="peer", events=events)
    finish_event = asyncio.Event()
    runtime = build_runtime(listener, runtime_executor)

    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await listener.listen_started.wait()

    assert listener.started
    assert not peer.started

    finish_event.set()
    await runtime_task

    assert [event for event in events if ".startup." in event] == [
        "listener.startup.worker",
    ]
    assert [event for event in events if event.endswith(".shutdown")] == [
        "listener.shutdown",
    ]


async def test_listener_task_can_send_through_outbound_only_broker(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    outbound = RuntimeBroker(router=router, broker_name="outbound", events=events)
    parent_finished = asyncio.Event()

    @first.task(task_name="runtime.outbound_child")
    async def outbound_child() -> None:
        return None

    @first.task(task_name="runtime.outbound_parent")
    async def outbound_parent() -> None:
        await outbound_child.kiq()
        parent_finished.set()

    router.route_task(outbound_child, broker=outbound)
    finish_event = asyncio.Event()
    runtime = build_runtime((first, second), runtime_executor)
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await asyncio.gather(first.listen_started.wait(), second.listen_started.wait())

    await outbound_parent.kicker().with_broker(first).kiq()
    await asyncio.wait_for(parent_finished.wait(), timeout=1)
    finish_event.set()
    await runtime_task

    assert "outbound.kick" in events
    assert not outbound.listen_started.is_set()
    assert events.index("outbound.startup.client") < events.index("outbound.kick")
    assert events.index("outbound.kick") < events.index("outbound.shutdown")


async def test_runtime_rolls_back_partial_startup_in_reverse_order(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    startup_error = RuntimeTestError("second startup failed")
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(
        router=router,
        broker_name="second",
        events=events,
        startup_error=startup_error,
    )
    runtime = build_runtime((first, second), runtime_executor)

    with pytest.raises(RuntimeTestError) as exc_info:
        await runtime.run(asyncio.Event())

    assert exc_info.value is startup_error
    assert events == [
        "first.startup.worker",
        "second.startup.worker",
        "second.shutdown",
        "first.shutdown",
    ]
    assert not first.listen_started.is_set()
    assert not second.listen_started.is_set()


async def test_runtime_preserves_listener_failure_after_sibling_cleanup(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    listen_error = RuntimeTestError("first listener failed")
    first = RuntimeBroker(
        router=router,
        broker_name="first",
        events=events,
        listen_error=listen_error,
    )
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    runtime = build_runtime((first, second), runtime_executor)

    runtime_task = asyncio.create_task(runtime.run(asyncio.Event()))
    await asyncio.gather(first.listen_started.wait(), second.listen_started.wait())
    first.release_listener.set()

    with pytest.raises(RuntimeTestError) as exc_info:
        await runtime_task

    assert exc_info.value is listen_error
    assert first.iterator_closed.is_set()
    assert second.iterator_closed.is_set()
    assert [event for event in events if event.endswith(".shutdown")] == [
        "second.shutdown",
        "first.shutdown",
    ]


async def test_runtime_keeps_first_listener_error_over_sibling_stop_error(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    first_started = asyncio.Event()
    primary_error = RuntimeTestError("second listener failed")
    sibling_error = RuntimeTestError("first listener failed while stopping")

    class OrderedFailureReceiver(Receiver):
        async def listen(self, finish_event: asyncio.Event) -> None:
            if self.broker is first:
                first_started.set()
                await finish_event.wait()
                raise sibling_error

            await first_started.wait()
            raise primary_error

    runtime = build_runtime(
        (first, second),
        runtime_executor,
        receiver_type=OrderedFailureReceiver,
    )

    with pytest.raises(RuntimeTestError) as exc_info:
        await runtime.run(asyncio.Event())

    assert exc_info.value is primary_error


async def test_runtime_cancellation_closes_all_listeners_and_brokers(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    runtime = build_runtime((first, second), runtime_executor)

    runtime_task = asyncio.create_task(runtime.run(asyncio.Event()))
    await asyncio.gather(first.listen_started.wait(), second.listen_started.wait())
    runtime_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await runtime_task

    assert first.iterator_closed.is_set()
    assert second.iterator_closed.is_set()
    assert [event for event in events if event.endswith(".shutdown")] == [
        "second.shutdown",
        "first.shutdown",
    ]


async def test_runtime_shares_async_execution_limit_across_brokers(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    finish_event = asyncio.Event()
    release_tasks = asyncio.Event()
    first_started = asyncio.Event()
    all_finished = asyncio.Event()
    active_tasks = 0
    max_active_tasks = 0
    started_tasks = 0
    finished_tasks = 0

    @first.task(task_name="runtime.shared_limit")
    async def limited_task() -> None:
        nonlocal active_tasks, finished_tasks, max_active_tasks, started_tasks
        active_tasks += 1
        started_tasks += 1
        max_active_tasks = max(max_active_tasks, active_tasks)
        first_started.set()
        try:
            await release_tasks.wait()
        finally:
            active_tasks -= 1
            finished_tasks += 1
            if finished_tasks == 2:
                all_finished.set()

    runtime = build_runtime(
        (first, second),
        runtime_executor,
        options=runtime_options(max_async_tasks=1),
    )
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await asyncio.gather(first.listen_started.wait(), second.listen_started.wait())

    await limited_task.kicker().with_broker(first).kiq()
    await limited_task.kicker().with_broker(second).kiq()
    await first_started.wait()
    await asyncio.sleep(0.05)

    assert started_tasks == 1
    assert max_active_tasks == 1

    release_tasks.set()
    await asyncio.wait_for(all_finished.wait(), timeout=1)
    finish_event.set()
    await runtime_task

    assert max_active_tasks == 1


async def test_runtime_shares_prefetch_limit_across_brokers(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    prefetched: list[str] = []
    first.with_middlewares(PrefetchRecordingMiddleware("first", prefetched))
    second.with_middlewares(PrefetchRecordingMiddleware("second", prefetched))
    first_task_started = asyncio.Event()
    all_tasks_finished = asyncio.Event()
    release_tasks = asyncio.Event()
    finished_tasks = 0

    @first.task(task_name="runtime.shared_prefetch")
    async def limited_task() -> None:
        nonlocal finished_tasks
        first_task_started.set()
        await release_tasks.wait()
        finished_tasks += 1
        if finished_tasks == 4:
            all_tasks_finished.set()

    finish_event = asyncio.Event()
    runtime = build_runtime(
        (first, second),
        runtime_executor,
        options=runtime_options(max_async_tasks=1, max_prefetch=0),
    )
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await asyncio.gather(first.listen_started.wait(), second.listen_started.wait())

    for target_broker in (first, second, first, second):
        await limited_task.kicker().with_broker(target_broker).kiq()
    await first_task_started.wait()

    async def wait_for_two_prefetched_tasks() -> None:
        while len(prefetched) < 2:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait_for_two_prefetched_tasks(), timeout=1)
    await asyncio.sleep(0.05)

    assert len(prefetched) == 2

    release_tasks.set()
    await asyncio.wait_for(all_tasks_finished.wait(), timeout=1)
    finish_event.set()
    await runtime_task


async def test_runtime_shares_max_tasks_per_child_across_brokers(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    first_finished = asyncio.Event()
    all_finished = asyncio.Event()
    execution_count = 0

    @first.task(task_name="runtime.shared_task_count")
    async def counted_task() -> None:
        nonlocal execution_count
        execution_count += 1
        if execution_count == 1:
            first_finished.set()
        elif execution_count == 2:
            all_finished.set()

    finish_event = asyncio.Event()
    runtime = build_runtime(
        (first, second),
        runtime_executor,
        options=runtime_options(max_tasks_to_execute=2),
    )
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await asyncio.gather(first.listen_started.wait(), second.listen_started.wait())

    await counted_task.kicker().with_broker(first).kiq()
    await first_finished.wait()
    await counted_task.kicker().with_broker(second).kiq()

    await asyncio.wait_for(all_finished.wait(), timeout=1)
    await asyncio.wait_for(runtime_task, timeout=1)

    assert execution_count == 2
    assert finish_event.is_set()


async def test_task_count_stop_does_not_discard_eager_transport_delivery(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    broker = RuntimeBroker(
        router=TaskiqRouter(),
        broker_name="listener",
        events=events,
    )
    prefetch_hook_started = asyncio.Event()
    release_prefetch_hook = asyncio.Event()
    execution_count = 0

    class PausingPrefetchMiddleware(TaskiqMiddleware):
        async def on_prefetch_queue_add(self) -> None:
            prefetch_hook_started.set()
            await release_prefetch_hook.wait()

    broker.with_middlewares(PausingPrefetchMiddleware())

    @broker.task(task_name="runtime.no_eager_delivery_loss")
    async def counted_task() -> None:
        nonlocal execution_count
        execution_count += 1

    runtime = build_runtime(
        (broker,),
        runtime_executor,
        options=runtime_options(max_tasks_to_execute=1),
    )
    runtime_task = asyncio.create_task(runtime.run(asyncio.Event()))
    await broker.listen_started.wait()

    await counted_task.kiq()
    await prefetch_hook_started.wait()
    await counted_task.kiq()
    release_prefetch_hook.set()
    await asyncio.wait_for(runtime_task, timeout=1)

    assert execution_count == 1
    assert broker.queue.qsize() == 1


async def test_simultaneous_task_count_deliveries_have_bounded_overshoot(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    execution_count = 0

    @first.task(task_name="runtime.bounded_task_count_overshoot")
    async def counted_task() -> None:
        nonlocal execution_count
        execution_count += 1

    runtime = build_runtime(
        (first, second),
        runtime_executor,
        options=runtime_options(
            max_prefetch=1,
            max_tasks_to_execute=1,
        ),
    )
    runtime_task = asyncio.create_task(runtime.run(asyncio.Event()))
    await asyncio.gather(first.listen_started.wait(), second.listen_started.wait())

    await asyncio.gather(
        counted_task.kicker().with_broker(first).kiq(),
        counted_task.kicker().with_broker(second).kiq(),
    )
    await asyncio.wait_for(runtime_task, timeout=1)

    assert execution_count == 2
    assert first.queue.empty()
    assert second.queue.empty()


async def test_runtime_cancels_callback_after_graceful_timeout(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    broker = RuntimeBroker(router=router, broker_name="listener", events=events)
    task_started = asyncio.Event()
    task_cancelled = asyncio.Event()

    @broker.task(task_name="runtime.callback_timeout")
    async def blocked_task() -> None:
        task_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            task_cancelled.set()

    finish_event = asyncio.Event()
    runtime = build_runtime(
        (broker,),
        runtime_executor,
        options=runtime_options(
            wait_tasks_timeout=0.01,
            shutdown_timeout=0.1,
        ),
    )
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await broker.listen_started.wait()
    await blocked_task.kiq()
    await task_started.wait()

    finish_event.set()
    await asyncio.wait_for(runtime_task, timeout=1)

    assert task_cancelled.is_set()
    assert broker.iterator_closed.is_set()


async def test_runtime_waits_for_callbacks_without_configured_task_timeout(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    broker = RuntimeBroker(
        router=TaskiqRouter(),
        broker_name="listener",
        events=events,
    )
    task_started = asyncio.Event()
    release_task = asyncio.Event()
    task_cancelled = False

    @broker.task(task_name="runtime.unbounded_graceful_wait")
    async def blocked_task() -> None:
        nonlocal task_cancelled
        task_started.set()
        try:
            await release_task.wait()
        except asyncio.CancelledError:
            task_cancelled = True
            raise

    finish_event = asyncio.Event()
    runtime = build_runtime(
        (broker,),
        runtime_executor,
        options=runtime_options(
            wait_tasks_timeout=None,
            shutdown_timeout=0.01,
        ),
    )
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await broker.listen_started.wait()
    await blocked_task.kiq()
    await task_started.wait()

    finish_event.set()
    await asyncio.sleep(0.05)
    finished_before_release = runtime_task.done()
    release_task.set()
    await asyncio.wait_for(runtime_task, timeout=1)

    assert not finished_before_release
    assert not task_cancelled


async def test_runtime_cancellation_during_drain_awaits_callback_cleanup(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    broker = RuntimeBroker(
        router=TaskiqRouter(),
        broker_name="listener",
        events=events,
    )
    task_started = asyncio.Event()
    release_task = asyncio.Event()
    task_cleaned = asyncio.Event()

    @broker.task(task_name="runtime.cancel_during_drain")
    async def blocked_task() -> None:
        task_started.set()
        try:
            await release_task.wait()
        finally:
            task_cleaned.set()

    finish_event = asyncio.Event()
    runtime = build_runtime(
        (broker,),
        runtime_executor,
        options=runtime_options(
            wait_tasks_timeout=0.4,
            shutdown_timeout=0.05,
        ),
    )
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await broker.listen_started.wait()
    await blocked_task.kiq()
    await task_started.wait()

    finish_event.set()
    await asyncio.wait_for(runtime_task, timeout=1)
    cleaned_before_runtime_returned = task_cleaned.is_set()

    release_task.set()
    await asyncio.wait_for(task_cleaned.wait(), timeout=1)

    assert cleaned_before_runtime_returned


async def test_runtime_continues_cleanup_after_broker_shutdown_failure(
    runtime_executor: ThreadPoolExecutor,
    caplog: pytest.LogCaptureFixture,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    shutdown_error = RuntimeTestError("second shutdown failed")
    second = RuntimeBroker(
        router=router,
        broker_name="second",
        events=events,
        shutdown_error=shutdown_error,
    )
    finish_event = asyncio.Event()
    runtime = build_runtime((first, second), runtime_executor)
    caplog.set_level(logging.WARNING, logger="taskiq.worker.runtime")

    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await asyncio.gather(first.listen_started.wait(), second.listen_started.wait())
    finish_event.set()
    await runtime_task

    assert [event for event in events if event.endswith(".shutdown")] == [
        "second.shutdown",
        "first.shutdown",
    ]
    assert "second failed during shutdown" in caplog.text


async def test_runtime_bounds_cancellation_resistant_broker_shutdown(
    runtime_executor: ThreadPoolExecutor,
    caplog: pytest.LogCaptureFixture,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    release_shutdown = asyncio.Event()
    shutdown_cancelled = asyncio.Event()
    shutdown_finished = asyncio.Event()

    class CancellationResistantShutdownBroker(RuntimeBroker):
        async def shutdown(self) -> None:
            self.events.append(f"{self.broker_name}.shutdown")
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                shutdown_cancelled.set()
                await release_shutdown.wait()
            finally:
                shutdown_finished.set()

    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = CancellationResistantShutdownBroker(
        router=router,
        broker_name="second",
        events=events,
    )
    runtime = build_runtime(
        (first, second),
        runtime_executor,
        options=runtime_options(shutdown_timeout=0.01),
    )
    caplog.set_level(logging.WARNING, logger="taskiq.worker.runtime")
    finish_event = asyncio.Event()
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await asyncio.gather(first.listen_started.wait(), second.listen_started.wait())

    finish_event.set()
    completed, _ = await asyncio.wait((runtime_task,), timeout=1)

    release_shutdown.set()
    if runtime_task not in completed:
        await asyncio.wait_for(runtime_task, timeout=0.2)
    else:
        await runtime_task
    await asyncio.wait_for(shutdown_finished.wait(), timeout=0.2)

    assert shutdown_cancelled.is_set()
    assert runtime_task in completed
    assert [event for event in events if event.endswith(".shutdown")] == [
        "second.shutdown",
        "first.shutdown",
    ]
    assert "second did not shut down" in caplog.text


async def test_runtime_cancellation_during_broker_shutdown_cleans_remaining_brokers(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    shutdown_started = asyncio.Event()

    class BlockingShutdownBroker(RuntimeBroker):
        async def shutdown(self) -> None:
            self.events.append(f"{self.broker_name}.shutdown")
            shutdown_started.set()
            await asyncio.Event().wait()

    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = BlockingShutdownBroker(
        router=router,
        broker_name="second",
        events=events,
    )
    runtime = build_runtime((first, second), runtime_executor)
    finish_event = asyncio.Event()
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await asyncio.gather(first.listen_started.wait(), second.listen_started.wait())

    finish_event.set()
    await shutdown_started.wait()
    runtime_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await runtime_task

    assert [event for event in events if event.endswith(".shutdown")] == [
        "second.shutdown",
        "first.shutdown",
    ]


async def test_runtime_does_not_swallow_fatal_broker_shutdown_error(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    shutdown_error = FatalRuntimeTestError("fatal shutdown")
    broker = RuntimeBroker(
        router=TaskiqRouter(),
        broker_name="listener",
        events=events,
        shutdown_error=shutdown_error,
    )
    runtime = build_runtime((broker,), runtime_executor)
    finish_event = asyncio.Event()
    finish_event.set()

    with pytest.raises(FatalRuntimeTestError) as exc_info:
        await runtime.run(finish_event)

    assert exc_info.value is shutdown_error


async def test_runtime_bounds_cancellation_resistant_listener_cleanup(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    listeners_started = {
        "first": asyncio.Event(),
        "second": asyncio.Event(),
    }
    cancellation_seen = asyncio.Event()
    release_listener = asyncio.Event()
    listener_tasks: list[asyncio.Task[None]] = []

    class CancellationResistantReceiver(Receiver):
        async def listen(self, finish_event: asyncio.Event) -> None:
            listeners_started[self.broker.broker_name].set()
            listener_task = asyncio.current_task()
            assert listener_task is not None
            listener_tasks.append(listener_task)
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_seen.set()
                await release_listener.wait()

    runtime = build_runtime(
        (first, second),
        runtime_executor,
        options=runtime_options(
            wait_tasks_timeout=0.01,
            shutdown_timeout=0.01,
        ),
        receiver_type=CancellationResistantReceiver,
    )

    finish_event = asyncio.Event()
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await asyncio.gather(
        listeners_started["first"].wait(),
        listeners_started["second"].wait(),
    )
    finish_event.set()
    await asyncio.wait_for(runtime_task, timeout=1)

    assert cancellation_seen.is_set()
    assert [event for event in events if event.endswith(".shutdown")] == [
        "second.shutdown",
        "first.shutdown",
    ]

    release_listener.set()
    await asyncio.wait_for(asyncio.gather(*listener_tasks), timeout=0.2)


def test_runtime_rejects_mixed_routers_before_startup(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    first = RuntimeBroker(
        router=TaskiqRouter(),
        broker_name="first",
        events=events,
    )
    second = RuntimeBroker(
        router=TaskiqRouter(),
        broker_name="second",
        events=events,
    )

    with pytest.raises(WorkerRuntimeConfigurationError, match="same router"):
        build_runtime((first, second), runtime_executor)

    assert events == []


@pytest.mark.parametrize(
    ("source", "error_match"),
    [
        ((), "cannot be empty"),
        ((object(),), "index 0"),
        ("module:broker", "must be an AsyncBroker"),
    ],
)
def test_runtime_rejects_invalid_listener_sources(
    source: object,
    error_match: str,
) -> None:
    with pytest.raises(WorkerRuntimeConfigurationError, match=error_match):
        normalize_broker_source(source)


def test_runtime_rejects_duplicate_listener_before_startup() -> None:
    events: list[str] = []
    broker = RuntimeBroker(
        router=TaskiqRouter(),
        broker_name="listener",
        events=events,
    )

    with pytest.raises(WorkerRuntimeConfigurationError, match="more than once"):
        normalize_broker_source((broker, broker))

    assert events == []


def test_single_broker_selection_does_not_change_router_peer_mode() -> None:
    events: list[str] = []
    router = TaskiqRouter()
    listener = RuntimeBroker(router=router, broker_name="listener", events=events)
    peer = RuntimeBroker(router=router, broker_name="peer", events=events)
    peer.is_worker_process = True

    normalize_broker_source(listener).mark_listener_brokers()

    assert listener.is_worker_process
    assert peer.is_worker_process


def test_runtime_rejects_shared_middleware_before_startup(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    middleware = TaskiqMiddleware()
    first.with_middlewares(middleware)
    second.with_middlewares(middleware)

    with pytest.raises(WorkerRuntimeConfigurationError, match="middleware"):
        build_runtime((first, second), runtime_executor)

    assert events == []


def test_runtime_rejects_shared_result_backend_before_startup(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)
    backend: DummyResultBackend[Any] = DummyResultBackend()
    first.with_result_backend(backend)
    second.with_result_backend(backend)

    with pytest.raises(WorkerRuntimeConfigurationError, match="result backend"):
        build_runtime((first, second), runtime_executor)

    assert events == []


def test_runtime_rejects_conflicting_effective_task_before_startup(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    router = TaskiqRouter()
    first = RuntimeBroker(router=router, broker_name="first", events=events)
    second = RuntimeBroker(router=router, broker_name="second", events=events)

    @first.task(task_name="runtime.conflict")
    async def first_task() -> str:
        return "first"

    @task_builder("runtime.conflict")
    async def second_task() -> str:
        return "second"

    second.store_registered_task(
        second.bind_task_definition(second_task, register=False),
    )

    with pytest.raises(WorkerRuntimeConfigurationError, match="resolves differently"):
        build_runtime((first, second), runtime_executor)

    assert events == []


async def test_runtime_rejects_custom_receiver_without_shared_state(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    broker = RuntimeBroker(
        router=TaskiqRouter(),
        broker_name="listener",
        events=events,
    )

    class IncompatibleReceiver(Receiver):
        def attach_runtime_state(
            self,
            runtime_state: ReceiverRuntimeState,
        ) -> ReceiverRuntimeState:
            return runtime_state

    runtime = build_runtime(
        (broker,),
        runtime_executor,
        receiver_type=IncompatibleReceiver,
    )

    with pytest.raises(WorkerRuntimeConfigurationError, match="Custom Receiver"):
        await runtime.run(asyncio.Event())

    assert events == []
