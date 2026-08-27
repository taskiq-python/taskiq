import asyncio
from contextvars import ContextVar
from typing import Any

import pytest

from taskiq import InMemoryBroker, TaskiqMessage
from taskiq.exceptions import SendTaskError, UnknownTaskError
from taskiq.kicker import AsyncKicker
from tests.brokers.inmemory_contract_support import (
    BlockingAfterAcceptKickInMemoryBroker,
    BlockingPostSendMiddleware,
    ChildReentrantDrainMiddleware,
    CoordinatedPostSendMiddleware,
    DeferredChildDrainMiddleware,
    DrainSignallingInMemoryBroker,
    FailingAfterAcceptKickInMemoryBroker,
    FailingExecutionMiddleware,
    FailingPostSendMiddleware,
    LifecycleError,
    PostSendError,
    RecordingKickInMemoryBroker,
    ReentrantDrainMiddleware,
    RejectingKickInMemoryBroker,
    ReplacingKickInMemoryBroker,
    TransformingKickInMemoryBroker,
)


async def test_direct_kick_preserves_inline_execution() -> None:
    task_executed = False
    broker = InMemoryBroker(await_inplace=True)

    @broker.task(task_name="direct.task")
    async def task() -> None:
        nonlocal task_executed
        task_executed = True

    message = TaskiqMessage(
        task_id="direct-task-id",
        task_name=task.task_name,
        labels={},
        labels_types={},
        args=[],
        kwargs={},
    )
    await broker.kick(broker.formatter.dumps(message))

    assert task_executed
    await broker.wait_all()
    await broker.shutdown()


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_kicker_uses_inmemory_public_kick_extension(
    await_inplace: bool,
) -> None:
    task_executed = False
    broker = RecordingKickInMemoryBroker(await_inplace=await_inplace)

    @broker.task
    async def task() -> None:
        nonlocal task_executed
        task_executed = True

    await task.kiq()
    await broker.wait_all()

    assert broker.kick_calls == 1
    assert task_executed
    await broker.shutdown()


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_cancelled_public_kick_preserves_accepted_execution(
    await_inplace: bool,
) -> None:
    events: list[str] = []
    post_send_started = asyncio.Event()
    release_post_send = asyncio.Event()
    task_executed = asyncio.Event()
    broker = BlockingAfterAcceptKickInMemoryBroker(
        await_inplace=await_inplace,
    ).with_middlewares(
        BlockingPostSendMiddleware(
            events,
            post_send_started,
            release_post_send,
        ),
    )

    @broker.task
    async def task() -> None:
        task_executed.set()

    send_task = asyncio.create_task(task.kiq())
    await asyncio.wait_for(broker.accepted.wait(), timeout=1)
    send_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await send_task

    await asyncio.wait_for(post_send_started.wait(), timeout=1)
    assert not task_executed.is_set()
    release_post_send.set()
    await asyncio.wait_for(broker.wait_all(), timeout=1)

    assert task_executed.is_set()
    assert events == [
        "pre_send",
        "post_send.started",
        "post_send.finished",
        "pre_execute",
        "post_execute",
    ]
    await broker.shutdown()


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_failed_public_kick_preserves_accepted_execution(
    await_inplace: bool,
) -> None:
    events: list[str] = []
    post_send_started = asyncio.Event()
    release_post_send = asyncio.Event()
    kick_error = LifecycleError("public kick failed after acceptance")
    task_executed = asyncio.Event()
    broker = FailingAfterAcceptKickInMemoryBroker(
        kick_error,
        await_inplace=await_inplace,
    ).with_middlewares(
        BlockingPostSendMiddleware(
            events,
            post_send_started,
            release_post_send,
        ),
    )

    @broker.task
    async def task() -> None:
        task_executed.set()

    with pytest.raises(SendTaskError) as exc_info:
        await task.kiq()

    await asyncio.wait_for(post_send_started.wait(), timeout=1)
    assert not task_executed.is_set()
    release_post_send.set()
    await asyncio.wait_for(broker.wait_all(), timeout=1)

    assert exc_info.value.__cause__ is kick_error
    assert task_executed.is_set()
    assert events == [
        "pre_send",
        "post_send.started",
        "post_send.finished",
        "pre_execute",
        "post_execute",
    ]
    await broker.shutdown()


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_public_kick_dispatches_the_message_accepted_by_base(
    await_inplace: bool,
) -> None:
    executed_tasks: list[str] = []
    broker = TransformingKickInMemoryBroker(
        "replacement",
        await_inplace=await_inplace,
    )

    @broker.task(task_name="original")
    async def original() -> None:
        executed_tasks.append("original")

    @broker.task(task_name="replacement")
    async def replacement() -> None:
        executed_tasks.append("replacement")

    await original.kiq()
    await broker.wait_all()

    assert executed_tasks == ["replacement"]
    await broker.shutdown()


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_async_post_send_finishes_before_execution(
    await_inplace: bool,
) -> None:
    events: list[str] = []
    post_send_started = asyncio.Event()
    release_post_send = asyncio.Event()
    task_started = asyncio.Event()
    broker = RecordingKickInMemoryBroker(await_inplace=await_inplace)
    broker.with_middlewares(
        BlockingPostSendMiddleware(
            events,
            post_send_started,
            release_post_send,
        ),
    )

    @broker.task
    async def task() -> None:
        events.append("task")
        task_started.set()

    kiq_task = asyncio.create_task(task.kiq())
    await asyncio.wait_for(post_send_started.wait(), timeout=1)

    assert broker.kick_calls == 1
    assert not task_started.is_set()
    assert events == ["pre_send", "post_send.started"]

    release_post_send.set()
    await asyncio.wait_for(kiq_task, timeout=1)
    await asyncio.wait_for(task_started.wait(), timeout=1)
    await broker.shutdown()

    assert events == [
        "pre_send",
        "post_send.started",
        "post_send.finished",
        "pre_execute",
        "task",
        "post_execute",
    ]


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_rejected_public_kick_skips_post_send(
    await_inplace: bool,
) -> None:
    events: list[str] = []
    post_send_started = asyncio.Event()
    release_post_send = asyncio.Event()
    release_post_send.set()
    kick_error = LifecycleError("public kick rejected the send")
    task_executed = False
    broker = RejectingKickInMemoryBroker(
        kick_error,
        await_inplace=await_inplace,
    ).with_middlewares(
        BlockingPostSendMiddleware(
            events,
            post_send_started,
            release_post_send,
        ),
    )

    @broker.task
    async def task() -> None:
        nonlocal task_executed
        task_executed = True

    with pytest.raises(SendTaskError) as exc_info:
        await task.kiq()

    assert exc_info.value.__cause__ is kick_error
    assert broker.kick_calls == 1
    assert events == ["pre_send"]
    assert not post_send_started.is_set()
    assert not task_executed
    await broker.shutdown()


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_replaced_public_kick_skips_local_execution(
    await_inplace: bool,
) -> None:
    events: list[str] = []
    post_send_started = asyncio.Event()
    release_post_send = asyncio.Event()
    release_post_send.set()
    task_executed = False
    broker = ReplacingKickInMemoryBroker(
        await_inplace=await_inplace,
    ).with_middlewares(
        BlockingPostSendMiddleware(
            events,
            post_send_started,
            release_post_send,
        ),
    )

    @broker.task
    async def task() -> None:
        nonlocal task_executed
        task_executed = True

    await task.kiq()

    assert broker.kick_calls == 1
    assert events == [
        "pre_send",
        "post_send.started",
        "post_send.finished",
    ]
    assert post_send_started.is_set()
    assert not task_executed
    assert not broker._running_tasks
    await broker.shutdown()


async def test_concurrent_inline_sends_keep_per_invocation_ownership() -> None:
    first_post_send_started = asyncio.Event()
    second_post_send_started = asyncio.Event()
    release_second_post_send = asyncio.Event()
    first_task_finished = asyncio.Event()
    second_task_started = asyncio.Event()
    finish_second_task = asyncio.Event()
    second_task_finished = asyncio.Event()
    broker = InMemoryBroker(await_inplace=True)

    @broker.task(task_name="first.task")
    async def first_task() -> None:
        first_task_finished.set()

    @broker.task(task_name="second.task")
    async def second_task() -> None:
        second_task_started.set()
        await finish_second_task.wait()
        second_task_finished.set()

    broker.with_middlewares(
        CoordinatedPostSendMiddleware(
            first_task.task_name,
            second_task.task_name,
            first_post_send_started,
            second_post_send_started,
            release_second_post_send,
        ),
    )

    first_sender = asyncio.create_task(first_task.kiq())
    await asyncio.wait_for(first_post_send_started.wait(), timeout=1)
    second_sender = asyncio.create_task(second_task.kiq())
    await asyncio.wait_for(second_post_send_started.wait(), timeout=1)

    await asyncio.wait_for(first_sender, timeout=1)
    assert first_task_finished.is_set()
    assert not second_task_started.is_set()
    assert not second_sender.done()

    release_second_post_send.set()
    await asyncio.wait_for(second_task_started.wait(), timeout=1)
    assert not second_sender.done()

    finish_second_task.set()
    await asyncio.wait_for(second_sender, timeout=1)

    assert second_task_finished.is_set()
    await broker.shutdown()


async def test_inline_send_preserves_caller_task_context() -> None:
    context_value: ContextVar[str] = ContextVar(
        "inmemory_inline_context",
        default="caller",
    )
    caller_task = asyncio.current_task()
    execution_task: asyncio.Task[Any] | None = None
    broker = InMemoryBroker(await_inplace=True)

    @broker.task
    async def task() -> None:
        nonlocal execution_task
        execution_task = asyncio.current_task()
        context_value.set("execution")

    await task.kiq()

    assert execution_task is caller_task
    assert context_value.get() == "execution"
    await broker.shutdown()


async def test_post_send_failure_does_not_retract_accepted_inline_task() -> None:
    post_send_error = PostSendError("post-send failed")
    task_executed = False
    broker = InMemoryBroker(await_inplace=True)
    broker.with_middlewares(FailingPostSendMiddleware(post_send_error))

    @broker.task
    async def task() -> None:
        nonlocal task_executed
        task_executed = True

    with pytest.raises(PostSendError) as exc_info:
        await task.kiq()

    assert exc_info.value is post_send_error
    assert task_executed
    await broker.shutdown()


async def test_sender_cancellation_after_post_send_failure_keeps_execution() -> None:
    post_send_error = PostSendError("post-send failed")
    task_started = asyncio.Event()
    finish_task = asyncio.Event()
    task_cancelled = False
    task_finished = False
    broker = InMemoryBroker(await_inplace=True)
    broker.with_middlewares(FailingPostSendMiddleware(post_send_error))

    @broker.task
    async def task() -> None:
        nonlocal task_cancelled, task_finished
        task_started.set()
        try:
            await finish_task.wait()
        except asyncio.CancelledError:
            task_cancelled = True
            raise
        task_finished = True

    sender = asyncio.create_task(task.kiq())
    await asyncio.wait_for(task_started.wait(), timeout=1)
    sender.cancel()

    with pytest.raises(asyncio.CancelledError):
        await sender

    assert not task_cancelled
    finish_task.set()
    await asyncio.wait_for(broker.wait_all(), timeout=1)
    assert task_finished
    await broker.shutdown()


async def test_inline_execution_failure_remains_a_send_error() -> None:
    execution_error = LifecycleError("execution failed")
    broker = InMemoryBroker(await_inplace=True)
    broker.with_middlewares(FailingExecutionMiddleware(execution_error))

    @broker.task
    async def task() -> None:
        return None

    with pytest.raises(SendTaskError) as exc_info:
        await task.kiq()

    assert exc_info.value.__cause__ is execution_error
    await broker.wait_all()
    await broker.shutdown()


async def test_kicker_wraps_unknown_inmemory_task() -> None:
    broker = InMemoryBroker()
    kicker: AsyncKicker[Any, Any] = AsyncKicker("missing.task", broker, {})

    with pytest.raises(SendTaskError) as exc_info:
        await kicker.kiq()

    assert isinstance(exc_info.value.__cause__, UnknownTaskError)
    await broker.shutdown()


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_post_send_failure_preserves_execution_error_ownership(
    await_inplace: bool,
) -> None:
    post_send_error = PostSendError("post-send failed")
    execution_error = LifecycleError("execution failed")
    broker = InMemoryBroker(await_inplace=await_inplace)
    broker.with_middlewares(
        FailingPostSendMiddleware(post_send_error),
        FailingExecutionMiddleware(execution_error),
    )

    @broker.task
    async def task() -> None:
        return None

    with pytest.raises(PostSendError) as exc_info:
        await task.kiq()

    assert exc_info.value is post_send_error
    if await_inplace:
        assert exc_info.value.__cause__ is execution_error
        await broker.wait_all()
    else:
        with pytest.raises(LifecycleError) as execution_exc_info:
            await broker.wait_all()
        assert execution_exc_info.value is execution_error

    await broker.shutdown()


async def test_post_send_failure_remains_primary_when_execution_is_cancelled() -> None:
    post_send_error = PostSendError("post-send failed")
    execution_cancel = asyncio.CancelledError("execution cancelled")
    broker = InMemoryBroker(await_inplace=True)
    broker.with_middlewares(
        FailingPostSendMiddleware(post_send_error),
        FailingExecutionMiddleware(execution_cancel),
    )

    @broker.task
    async def task() -> None:
        return None

    with pytest.raises(PostSendError) as exc_info:
        await task.kiq()

    assert exc_info.value is post_send_error
    assert isinstance(exc_info.value.__cause__, asyncio.CancelledError)
    await broker.wait_all()
    await broker.shutdown()


async def test_post_send_cancellation_does_not_retract_accepted_inline_task() -> None:
    events: list[str] = []
    post_send_started = asyncio.Event()
    release_post_send = asyncio.Event()
    task_executed = asyncio.Event()
    broker = InMemoryBroker(await_inplace=True)
    broker.with_middlewares(
        BlockingPostSendMiddleware(
            events,
            post_send_started,
            release_post_send,
        ),
    )

    @broker.task
    async def task() -> None:
        task_executed.set()

    kiq_task = asyncio.create_task(task.kiq())
    await asyncio.wait_for(post_send_started.wait(), timeout=1)
    kiq_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await kiq_task

    await asyncio.wait_for(task_executed.wait(), timeout=1)
    await asyncio.wait_for(broker.wait_all(), timeout=1)
    await broker.shutdown()


async def test_sender_cancellation_preserves_inline_execution_cancellation() -> None:
    task_started = asyncio.Event()
    task_cancelled = asyncio.Event()
    keep_running = asyncio.Event()
    broker = InMemoryBroker(await_inplace=True)

    @broker.task
    async def task() -> None:
        task_started.set()
        try:
            await keep_running.wait()
        except asyncio.CancelledError:
            task_cancelled.set()
            raise

    kiq_task = asyncio.create_task(task.kiq())
    await asyncio.wait_for(task_started.wait(), timeout=1)
    kiq_task.cancel()

    await asyncio.wait_for(asyncio.shield(kiq_task), timeout=1)

    assert not kiq_task.cancelled()
    assert task_cancelled.is_set()
    await broker.wait_all()
    await broker.shutdown()


async def test_shutdown_waits_for_send_blocked_in_post_send() -> None:
    events: list[str] = []
    post_send_started = asyncio.Event()
    release_post_send = asyncio.Event()
    task_executed = asyncio.Event()
    drain_started = asyncio.Event()
    broker = DrainSignallingInMemoryBroker(drain_started)
    broker.with_middlewares(
        BlockingPostSendMiddleware(
            events,
            post_send_started,
            release_post_send,
        ),
    )

    @broker.task
    async def task() -> None:
        task_executed.set()

    kiq_task = asyncio.create_task(task.kiq())
    await asyncio.wait_for(post_send_started.wait(), timeout=1)
    shutdown_task = asyncio.create_task(broker.shutdown())
    await asyncio.wait_for(drain_started.wait(), timeout=1)

    assert not shutdown_task.done()
    assert not task_executed.is_set()

    release_post_send.set()
    await kiq_task
    await asyncio.wait_for(shutdown_task, timeout=1)

    assert task_executed.is_set()


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_cancelled_wait_all_does_not_cancel_accepted_send(
    await_inplace: bool,
) -> None:
    events: list[str] = []
    post_send_started = asyncio.Event()
    release_post_send = asyncio.Event()
    task_executed = asyncio.Event()
    drain_started = asyncio.Event()
    broker = DrainSignallingInMemoryBroker(
        drain_started,
        await_inplace=await_inplace,
    )
    broker.with_middlewares(
        BlockingPostSendMiddleware(
            events,
            post_send_started,
            release_post_send,
        ),
    )

    @broker.task
    async def task() -> None:
        task_executed.set()

    sender = asyncio.create_task(task.kiq())
    await asyncio.wait_for(post_send_started.wait(), timeout=1)
    drain = asyncio.create_task(broker.wait_all())
    await asyncio.wait_for(drain_started.wait(), timeout=1)
    drain.cancel()

    with pytest.raises(asyncio.CancelledError):
        await drain

    assert not sender.done()
    assert not task_executed.is_set()

    release_post_send.set()
    await asyncio.wait_for(sender, timeout=1)
    await asyncio.wait_for(task_executed.wait(), timeout=1)
    await asyncio.wait_for(broker.wait_all(), timeout=1)
    await broker.shutdown()


@pytest.mark.parametrize("operation", ["wait_all", "shutdown"])
@pytest.mark.parametrize("await_inplace", [False, True])
async def test_execution_cannot_reenter_broker_drain(
    operation: str,
    await_inplace: bool,
) -> None:
    broker = InMemoryBroker(await_inplace=await_inplace)
    drain_error: RuntimeError | None = None

    @broker.task
    async def task() -> None:
        nonlocal drain_error
        try:
            await getattr(broker, operation)()
        except RuntimeError as exc:
            drain_error = exc

    await asyncio.wait_for(task.kiq(), timeout=1)
    await asyncio.wait_for(broker.wait_all(), timeout=1)

    assert drain_error is not None
    assert str(drain_error).startswith(f"InMemoryBroker.{operation}()")
    assert broker.executor.submit(int).result() == 0
    await broker.shutdown()


@pytest.mark.parametrize("operation", ["wait_all", "shutdown"])
@pytest.mark.parametrize("await_inplace", [False, True])
async def test_post_send_cannot_reenter_broker_drain(
    operation: str,
    await_inplace: bool,
) -> None:
    task_executed = False
    broker = InMemoryBroker(await_inplace=await_inplace)
    broker.with_middlewares(ReentrantDrainMiddleware(operation))

    @broker.task
    async def task() -> None:
        nonlocal task_executed
        task_executed = True

    with pytest.raises(RuntimeError) as exc_info:
        await asyncio.wait_for(task.kiq(), timeout=1)

    await asyncio.wait_for(broker.wait_all(), timeout=1)
    assert str(exc_info.value).startswith(f"InMemoryBroker.{operation}()")
    assert task_executed
    assert broker.executor.submit(int).result() == 0
    await broker.shutdown()


@pytest.mark.parametrize("operation", ["wait_all", "shutdown"])
@pytest.mark.parametrize("await_inplace", [False, True])
async def test_post_send_child_cannot_reenter_broker_drain(
    operation: str,
    await_inplace: bool,
) -> None:
    task_executed = False
    broker = InMemoryBroker(await_inplace=await_inplace)
    broker.with_middlewares(ChildReentrantDrainMiddleware(operation))

    @broker.task
    async def task() -> None:
        nonlocal task_executed
        task_executed = True

    with pytest.raises(RuntimeError) as exc_info:
        await asyncio.wait_for(task.kiq(), timeout=1)

    await asyncio.wait_for(broker.wait_all(), timeout=1)
    assert str(exc_info.value).startswith(f"InMemoryBroker.{operation}()")
    assert task_executed
    assert broker.executor.submit(int).result() == 0
    await broker.shutdown()


@pytest.mark.parametrize("await_inplace", [False, True])
async def test_post_send_child_can_drain_after_send_lifecycle(
    await_inplace: bool,
) -> None:
    release = asyncio.Event()
    broker = InMemoryBroker(await_inplace=await_inplace)
    middleware = DeferredChildDrainMiddleware(release)
    broker.with_middlewares(middleware)

    @broker.task
    async def task() -> None:
        return None

    await asyncio.wait_for(task.kiq(), timeout=1)
    await asyncio.wait_for(broker.wait_all(), timeout=1)
    release.set()

    assert middleware.drain_task is not None
    assert await asyncio.wait_for(middleware.drain_task, timeout=1) is None
    await broker.shutdown()
