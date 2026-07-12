import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

import pytest

from taskiq import TaskiqRouter
from taskiq.receiver import Receiver
from taskiq.receiver.runtime import (
    ReceiverRuntimeState,
    WorkerRuntime,
    WorkerRuntimeConfigurationError,
    normalize_broker_source,
)
from tests.receiver.runtime_support import (
    RuntimeBroker,
    RuntimeTestError,
    build_runtime,
    runtime_options,
)


async def test_runtime_propagates_adapter_shutdown_cancellation(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    shutdown_error = asyncio.CancelledError("adapter shutdown cancelled")
    broker = RuntimeBroker(
        router=TaskiqRouter(),
        broker_name="listener",
        events=events,
        shutdown_error=shutdown_error,
    )
    runtime = build_runtime((broker,), runtime_executor)
    finish_event = asyncio.Event()
    finish_event.set()

    with pytest.raises(asyncio.CancelledError):
        await runtime.run(finish_event)
    assert events[-1] == "listener.shutdown"


async def test_runtime_logs_shutdown_failure_after_cleanup_deadline(
    runtime_executor: ThreadPoolExecutor,
    caplog: pytest.LogCaptureFixture,
) -> None:
    events: list[str] = []
    release_shutdown = asyncio.Event()
    shutdown_finished = asyncio.Event()
    late_error = RuntimeTestError("late shutdown failure")

    class LateFailureShutdownBroker(RuntimeBroker):
        async def shutdown(self) -> None:
            self.events.append(f"{self.broker_name}.shutdown")
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release_shutdown.wait()
                raise late_error from None
            finally:
                shutdown_finished.set()

    broker = LateFailureShutdownBroker(
        router=TaskiqRouter(),
        broker_name="listener",
        events=events,
    )
    runtime = build_runtime(
        (broker,),
        runtime_executor,
        options=runtime_options(shutdown_timeout=0.01),
    )
    finish_event = asyncio.Event()
    caplog.set_level(logging.ERROR, logger="taskiq.worker.runtime")
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await broker.listen_started.wait()

    finish_event.set()
    await asyncio.wait_for(runtime_task, timeout=1)
    release_shutdown.set()
    await asyncio.wait_for(shutdown_finished.wait(), timeout=1)
    await asyncio.sleep(0)

    assert "failed after its cleanup deadline" in caplog.text


async def test_runtime_preserves_listener_failure_discovered_during_settlement(
    runtime_executor: ThreadPoolExecutor,
) -> None:
    events: list[str] = []
    broker = RuntimeBroker(
        router=TaskiqRouter(),
        broker_name="listener",
        events=events,
    )
    finish_observed = asyncio.Event()
    settlement_started = asyncio.Event()
    release_failure = asyncio.Event()
    late_error = RuntimeTestError("listener failed while settling")

    class LateFailureReceiver(Receiver):
        async def listen(self, finish_event: asyncio.Event) -> None:
            await finish_event.wait()
            finish_observed.set()
            await release_failure.wait()
            raise late_error

    class ObservedSettlementRuntime(WorkerRuntime):
        async def _settle_listener_tasks(
            self,
            listener_tasks: tuple[asyncio.Task[None], ...],
            receivers: tuple[Receiver, ...],
        ) -> tuple[list[None | BaseException], set[asyncio.Task[None]]]:
            settlement_started.set()
            return await super()._settle_listener_tasks(listener_tasks, receivers)

    finish_event = asyncio.Event()
    runtime = ObservedSettlementRuntime(
        selection=normalize_broker_source((broker,)),
        receiver_type=LateFailureReceiver,
        executor=runtime_executor,
        options=runtime_options(),
    )
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    while not broker.started:
        await asyncio.sleep(0)

    finish_event.set()
    await finish_observed.wait()
    await settlement_started.wait()
    release_failure.set()

    with pytest.raises(RuntimeTestError) as exc_info:
        await runtime_task

    assert exc_info.value is late_error


async def test_runtime_logs_late_cancellation_resistant_listener_failure(
    runtime_executor: ThreadPoolExecutor,
    caplog: pytest.LogCaptureFixture,
) -> None:
    events: list[str] = []
    broker = RuntimeBroker(
        router=TaskiqRouter(),
        broker_name="listener",
        events=events,
    )
    listener_started = asyncio.Event()
    cancellation_seen = asyncio.Event()
    release_listener = asyncio.Event()
    late_error = RuntimeTestError("late listener failure")
    listener_tasks: list[asyncio.Task[None]] = []

    class LateFailureReceiver(Receiver):
        async def listen(self, finish_event: asyncio.Event) -> None:
            listener_task = asyncio.current_task()
            assert listener_task is not None
            listener_tasks.append(listener_task)
            listener_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_seen.set()
                await release_listener.wait()
                raise late_error from None

    runtime = build_runtime(
        (broker,),
        runtime_executor,
        options=runtime_options(
            wait_tasks_timeout=0.01,
            shutdown_timeout=0.01,
        ),
        receiver_type=LateFailureReceiver,
    )
    finish_event = asyncio.Event()
    caplog.set_level(logging.ERROR, logger="taskiq.worker.runtime")
    runtime_task = asyncio.create_task(runtime.run(finish_event))
    await listener_started.wait()

    finish_event.set()
    await asyncio.wait_for(runtime_task, timeout=1)
    assert cancellation_seen.is_set()

    release_listener.set()
    results = await asyncio.wait_for(
        asyncio.gather(*listener_tasks, return_exceptions=True),
        timeout=1,
    )
    await asyncio.sleep(0)

    assert results == [late_error]
    assert "failed after runtime cleanup" in caplog.text


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        pytest.param(
            {"max_prefetch": -1},
            "max_prefetch cannot be negative",
            id="negative-prefetch",
        ),
        pytest.param(
            {"max_async_tasks_jitter": -1},
            "max_async_tasks_jitter cannot be negative",
            id="negative-jitter",
        ),
        pytest.param(
            {"shutdown_timeout": -1},
            "shutdown_timeout cannot be negative",
            id="negative-shutdown-timeout",
        ),
        pytest.param(
            {"wait_tasks_timeout": -1},
            "wait_tasks_timeout cannot be negative",
            id="negative-task-timeout",
        ),
    ],
)
def test_runtime_options_reject_negative_limits(
    overrides: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(WorkerRuntimeConfigurationError, match=message):
        runtime_options(**overrides)


async def test_runtime_state_applies_shared_execution_jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "taskiq.receiver.runtime.random.randint",
        lambda start, end: 2,
    )
    runtime_state = ReceiverRuntimeState(
        finish_event=asyncio.Event(),
        max_async_tasks=1,
        max_async_tasks_jitter=2,
        max_prefetch=0,
        max_tasks_to_execute=None,
    )
    semaphore = runtime_state.execution_semaphore
    assert semaphore is not None

    for _ in range(3):
        await semaphore.acquire()

    assert semaphore.locked()


def test_runtime_state_preserves_unlimited_async_execution() -> None:
    runtime_state = ReceiverRuntimeState(
        finish_event=asyncio.Event(),
        max_async_tasks=None,
        max_async_tasks_jitter=0,
        max_prefetch=0,
        max_tasks_to_execute=None,
    )

    assert runtime_state.execution_semaphore is None
