import asyncio
import contextvars
import random
import time
import unittest.mock
from collections.abc import AsyncGenerator, Generator
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Any, ClassVar

import pytest
from taskiq_dependencies import Depends

from taskiq.abc.broker import AckableMessage, AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.acks import AcknowledgeType
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.context import Context
from taskiq.exceptions import NoResultError, TaskiqResultTimeoutError
from taskiq.message import TaskiqMessage
from taskiq.receiver import Receiver
from taskiq.result import TaskiqResult
from tests.utils import AsyncQueueBroker


def get_receiver(
    broker: AsyncBroker | None = None,
    no_parse: bool = False,
    max_async_tasks: int | None = None,
    max_async_tasks_jitter: int = 0,
    ack_type: AcknowledgeType | None = None,
) -> Receiver:
    """
    Returns receiver with custom broker and args.

    :param broker: broker, defaults to None
    :param no_parse: parameter to taskiq_args, defaults to False
    :param max_async_tasks: maximum number of simultaneous async tasks.
    :param max_async_tasks_jitter: random jitter to add to max_async_tasks.
    :return: new receiver.
    """
    if broker is None:
        broker = InMemoryBroker()
    return Receiver(
        broker,
        executor=ThreadPoolExecutor(max_workers=10),
        validate_params=not no_parse,
        max_async_tasks=max_async_tasks,
        max_async_tasks_jitter=max_async_tasks_jitter,
        ack_type=ack_type,
    )


class _EventResultBackend(AsyncResultBackend[Any]):
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.results: dict[str, TaskiqResult[Any]] = {}

    async def set_result(self, task_id: str, result: TaskiqResult[Any]) -> None:
        self.events.append("save")
        self.results[task_id] = result

    async def is_result_ready(self, task_id: str) -> bool:
        return task_id in self.results

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[Any]:
        return self.results[task_id]


class _EventMiddleware(TaskiqMiddleware):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events

    def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        self.events.append("post_execute")

    def post_save(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        self.events.append("post_save")


async def test_run_task_successful_async() -> None:
    """Tests that run_task can run async tasks."""

    async def test_func(param: int) -> int:
        return param

    receiver = get_receiver()

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={},
            args=[1],
            kwargs={},
        ),
    )

    assert result.return_value == 1


async def test_run_task_successful_sync() -> None:
    """Tests that run_task can run sync tasks."""

    def test_func(param: int) -> int:
        return param

    receiver = get_receiver()

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={},
            args=[1],
            kwargs={},
        ),
    )
    assert result.return_value == 1


async def test_run_task_exception() -> None:
    """Tests that run_task can run sync tasks."""

    def test_func() -> None:
        raise ValueError

    receiver = get_receiver()

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={},
            args=[],
            kwargs={},
        ),
    )
    assert result.return_value is None
    assert result.is_err


async def test_run_timeouts() -> None:
    async def test_func() -> None:
        await asyncio.sleep(2)

    receiver = get_receiver()

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={"timeout": "0.3"},
            args=[],
            kwargs={},
        ),
    )
    assert result.return_value is None
    assert result.execution_time < 2
    assert result.is_err


async def test_run_timeouts_sync() -> None:
    def test_func() -> None:
        time.sleep(2)

    receiver = get_receiver()

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={"timeout": "0.3"},
            args=[],
            kwargs={},
        ),
    )
    assert result.return_value is None
    assert result.execution_time < 2
    assert result.is_err


async def test_run_task_exception_middlewares() -> None:
    """Tests that run_task can run sync tasks."""

    class _TestMiddleware(TaskiqMiddleware):
        found_exceptions: ClassVar[list[BaseException]] = []

        def on_error(
            self,
            message: "TaskiqMessage",
            result: "TaskiqResult[Any]",
            exception: BaseException,
        ) -> None:
            self.found_exceptions.append(exception)

    def test_func() -> None:
        raise ValueError

    broker = InMemoryBroker().with_middlewares(_TestMiddleware())
    receiver = get_receiver(broker)

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={},
            args=[],
            kwargs={},
        ),
    )
    assert result.return_value is None
    assert result.is_err
    assert len(_TestMiddleware.found_exceptions) == 1
    assert _TestMiddleware.found_exceptions[0].__class__ is ValueError


async def test_callback_success() -> None:
    """Test that callback function works well."""
    broker = InMemoryBroker()
    called_times = 0

    @broker.task
    async def my_task() -> int:
        nonlocal called_times
        called_times += 1
        return 1

    receiver = get_receiver(broker)

    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name=my_task.task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    )

    await receiver.callback(broker_message.message)
    assert called_times == 1


async def test_callback_no_dep_info() -> None:
    """Test that callback function works well."""
    broker = InMemoryBroker()
    expected = random.randint(1, 100)
    ret_val = None

    def dependency() -> int:
        return expected

    @broker.task
    async def my_task(dep: int = Depends(dependency)) -> None:
        nonlocal ret_val
        ret_val = dep

    receiver = get_receiver(broker)
    receiver.known_tasks.remove(my_task.task_name)
    receiver.dependency_graphs.pop(my_task.task_name, None)
    receiver.task_signatures.pop(my_task.task_name, None)
    receiver.task_hints.pop(my_task.task_name, None)

    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name=my_task.task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    )

    await receiver.callback(broker_message.message)
    assert ret_val == expected


async def test_callback_success_ackable() -> None:
    """Test that acking works."""
    broker = InMemoryBroker()
    called_times = 0
    acked = False

    @broker.task
    async def my_task() -> int:
        nonlocal called_times
        called_times += 1
        return 1

    def ack_callback() -> None:
        nonlocal acked
        acked = True

    receiver = get_receiver(broker)

    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name=my_task.task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    )

    await receiver.callback(
        AckableMessage(
            data=broker_message.message,
            ack=ack_callback,
        ),
    )
    assert called_times == 1
    assert acked


async def test_callback_success_ackable_async() -> None:
    """Test that acks work with async functions."""
    broker = InMemoryBroker()
    called_times = 0
    acked = False

    @broker.task
    async def my_task() -> int:
        nonlocal called_times
        called_times += 1
        return 1

    async def ack_callback() -> None:
        nonlocal acked
        acked = True

    receiver = get_receiver(broker)

    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name=my_task.task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    )

    await receiver.callback(
        AckableMessage(
            data=broker_message.message,
            ack=ack_callback,
        ),
    )
    assert called_times == 1
    assert acked


async def test_task_ack_type_when_received_overrides_worker_ack_type() -> None:
    """Task ack_type label overrides worker-level ack type."""
    events: list[str] = []
    broker = (
        InMemoryBroker()
        .with_result_backend(
            _EventResultBackend(events),
        )
        .with_middlewares(_EventMiddleware(events))
    )

    @broker.task
    async def my_task() -> int:
        events.append("task")
        return 1

    def ack_callback() -> None:
        events.append("ack")

    receiver = get_receiver(broker, ack_type=AcknowledgeType.WHEN_SAVED)
    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name=my_task.task_name,
            labels={"ack_type": "when_received"},
            args=[],
            kwargs={},
        ),
    )

    await receiver.callback(
        AckableMessage(
            data=broker_message.message,
            ack=ack_callback,
        ),
    )

    assert events == ["ack", "task", "post_execute", "save", "post_save"]


async def test_task_ack_type_when_executed_overrides_worker_ack_type() -> None:
    """Task ack_type label can delay a worker-level when_received ack."""
    events: list[str] = []
    broker = (
        InMemoryBroker()
        .with_result_backend(
            _EventResultBackend(events),
        )
        .with_middlewares(_EventMiddleware(events))
    )

    @broker.task(ack_type="when_executed")
    async def my_task() -> int:
        events.append("task")
        return 1

    def ack_callback() -> None:
        events.append("ack")

    receiver = get_receiver(broker, ack_type=AcknowledgeType.WHEN_RECEIVED)
    broker_message = broker.formatter.dumps(my_task.kicker()._prepare_message())

    await receiver.callback(
        AckableMessage(
            data=broker_message.message,
            ack=ack_callback,
        ),
    )

    assert events == ["task", "ack", "post_execute", "save", "post_save"]


async def test_task_ack_type_when_saved_overrides_worker_ack_type() -> None:
    """Task ack_type label can use when_saved with an early-ack worker."""
    events: list[str] = []
    broker = (
        InMemoryBroker()
        .with_result_backend(
            _EventResultBackend(events),
        )
        .with_middlewares(_EventMiddleware(events))
    )

    @broker.task
    async def my_task() -> int:
        events.append("task")
        return 1

    def ack_callback() -> None:
        events.append("ack")

    receiver = get_receiver(broker, ack_type=AcknowledgeType.WHEN_RECEIVED)
    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name=my_task.task_name,
            labels={"ack_type": "when_saved"},
            args=[],
            kwargs={},
        ),
    )

    await receiver.callback(
        AckableMessage(
            data=broker_message.message,
            ack=ack_callback,
        ),
    )

    assert events == ["task", "post_execute", "save", "post_save", "ack"]


async def test_worker_ack_type_is_used_without_task_ack_type() -> None:
    """Worker-level ack_type is used when the task has no ack_type label."""
    events: list[str] = []
    broker = (
        InMemoryBroker()
        .with_result_backend(
            _EventResultBackend(events),
        )
        .with_middlewares(_EventMiddleware(events))
    )

    @broker.task
    async def my_task() -> int:
        events.append("task")
        return 1

    def ack_callback() -> None:
        events.append("ack")

    receiver = get_receiver(broker, ack_type=AcknowledgeType.WHEN_RECEIVED)
    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name=my_task.task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    )

    await receiver.callback(
        AckableMessage(
            data=broker_message.message,
            ack=ack_callback,
        ),
    )

    assert events == ["ack", "task", "post_execute", "save", "post_save"]


async def test_manual_task_ack_from_context() -> None:
    """Manual ack lets a task acknowledge the message through context."""
    events: list[str] = []
    broker = (
        InMemoryBroker()
        .with_result_backend(
            _EventResultBackend(events),
        )
        .with_middlewares(_EventMiddleware(events))
    )

    @broker.task(ack_type="manual")
    async def my_task(context: Context = Depends()) -> int:
        events.append("task")
        assert context.is_ackable
        assert not context.is_acked
        await context.ack()
        assert context.is_acked
        return 1

    def ack_callback() -> None:
        events.append("ack")

    receiver = get_receiver(broker, ack_type=AcknowledgeType.WHEN_SAVED)
    broker_message = broker.formatter.dumps(my_task.kicker()._prepare_message())

    await receiver.callback(
        AckableMessage(
            data=broker_message.message,
            ack=ack_callback,
        ),
    )

    assert events == ["task", "ack", "post_execute", "save", "post_save"]


async def test_manual_task_ack_is_idempotent() -> None:
    """Calling Context.ack twice acknowledges the message once."""
    events: list[str] = []
    broker = (
        InMemoryBroker()
        .with_result_backend(
            _EventResultBackend(events),
        )
        .with_middlewares(_EventMiddleware(events))
    )

    @broker.task(ack_type="manual")
    async def my_task(context: Context = Depends()) -> int:
        events.append("task")
        await context.ack()
        await context.ack()
        return 1

    def ack_callback() -> None:
        events.append("ack")

    receiver = get_receiver(broker, ack_type=AcknowledgeType.WHEN_SAVED)
    broker_message = broker.formatter.dumps(my_task.kicker()._prepare_message())

    await receiver.callback(
        AckableMessage(
            data=broker_message.message,
            ack=ack_callback,
        ),
    )

    assert events == ["task", "ack", "post_execute", "save", "post_save"]


async def test_manual_task_ack_without_ackable_message_saves_error() -> None:
    """Context.ack fails clearly when broker messages do not support acking."""
    events: list[str] = []
    result_backend = _EventResultBackend(events)
    broker = (
        InMemoryBroker()
        .with_result_backend(result_backend)
        .with_middlewares(_EventMiddleware(events))
    )

    @broker.task(ack_type="manual")
    async def my_task(context: Context = Depends()) -> int:
        assert not context.is_ackable
        await context.ack()
        return 1

    receiver = get_receiver(broker)
    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name=my_task.task_name,
            labels={"ack_type": "manual"},
            args=[],
            kwargs={},
        ),
    )

    await receiver.callback(broker_message.message)

    result = result_backend.results["task_id"]
    assert result.is_err
    assert isinstance(result.error, RuntimeError)
    assert str(result.error) == "Current message is not ackable."
    assert events == ["post_execute", "save", "post_save"]


async def test_invalid_task_ack_type_raises_before_execution() -> None:
    """Invalid task ack_type fails before task execution and acknowledgement."""
    events: list[str] = []
    broker = InMemoryBroker()

    @broker.task
    async def my_task() -> None:
        events.append("task")

    def ack_callback() -> None:
        events.append("ack")

    receiver = get_receiver(broker, ack_type=AcknowledgeType.WHEN_RECEIVED)
    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name=my_task.task_name,
            labels={"ack_type": "when_save"},
            args=[],
            kwargs={},
        ),
    )

    with pytest.raises(ValueError, match="Invalid ack_type label"):
        await receiver.callback(
            AckableMessage(
                data=broker_message.message,
                ack=ack_callback,
            ),
        )

    assert events == []


async def test_callback_wrong_format() -> None:
    """Test that wrong format of a message won't throw an error."""
    receiver = get_receiver()

    await receiver.callback(
        b"{some wrong bytes}",
    )


async def test_callback_unknown_task() -> None:
    """Tests that running an unknown task won't throw an error."""
    broker = InMemoryBroker()
    receiver = get_receiver(broker)

    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name="unknown",
            labels={},
            args=[],
            kwargs={},
        ),
    )

    await receiver.callback(broker_message.message)


async def test_custom_ctx() -> None:
    """Tests that run_task can run sync tasks."""

    class MyTestClass:
        """Class to test injection."""

        def __init__(self, val: int) -> None:
            self.val = val

    broker = InMemoryBroker()

    # We register a task into broker,
    # to build dependency graph on startup.
    @broker.task
    def test_func(tes_val: MyTestClass = Depends()) -> int:
        return tes_val.val

    # We add custom first-level dependency.
    broker.add_dependency_context({MyTestClass: MyTestClass(11)})
    # Create a receiver.
    receiver = get_receiver(broker)

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name=test_func.task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    )

    # Check that the value is equal
    # to the one we supplied.
    assert result.return_value == 11
    assert not result.is_err


async def test_callback_semaphore() -> None:
    """Test that callback function semaphore works well."""
    max_async_tasks = 3
    broker = AsyncQueueBroker()
    sem_num = 0

    @broker.task
    async def task_sem() -> int:
        nonlocal sem_num
        sem_num += 1
        await asyncio.sleep(1)
        return 1

    for _ in range(max_async_tasks + 2):
        await task_sem.kiq()
    receiver = get_receiver(broker, max_async_tasks=max_async_tasks)

    listen_task = asyncio.create_task(receiver.listen(asyncio.Event()))
    await asyncio.sleep(0.3)
    assert sem_num == max_async_tasks
    await broker.wait_tasks()
    assert sem_num == max_async_tasks + 2
    listen_task.cancel()


async def test_no_result_error() -> None:
    broker = InMemoryBroker()
    executed = asyncio.Event()

    @broker.task
    async def task_no_result() -> int:
        executed.set()
        raise NoResultError

    task = await task_no_result.kiq()
    with pytest.raises(TaskiqResultTimeoutError):
        await task.wait_result(timeout=1)

    assert executed.is_set()
    assert not broker._running_tasks


async def test_result() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def task_no_result() -> str:
        return "some value"

    task = await task_no_result.kiq()
    resp = await task.wait_result(timeout=1)

    assert resp.return_value == "some value"
    assert not broker._running_tasks


async def test_error_result() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def task_no_result() -> str:
        raise ValueError("some error")

    task = await task_no_result.kiq()
    resp = await task.wait_result(timeout=1)

    assert resp.return_value is None
    assert not broker._running_tasks
    assert isinstance(resp.error, ValueError)


EXPECTED_CTX_VALUE = 42


@pytest.fixture()
def ctxvar() -> Generator[contextvars.ContextVar[int], None, None]:
    _ctx_variable: contextvars.ContextVar[int] = contextvars.ContextVar(
        "taskiq_test_ctx_var",
    )
    token = _ctx_variable.set(EXPECTED_CTX_VALUE)
    yield _ctx_variable
    _ctx_variable.reset(token)


async def test_run_task_successful_sync_preserve_contextvars(
    ctxvar: contextvars.ContextVar[int],
) -> None:
    """Running sync tasks should preserve context vars."""

    def test_func() -> int:
        return ctxvar.get()

    receiver = get_receiver()

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={},
            args=[],
            kwargs={},
        ),
    )
    assert result.return_value == EXPECTED_CTX_VALUE


async def test_run_task_successful_async_preserve_contextvars(
    ctxvar: contextvars.ContextVar[int],
) -> None:
    """Running async tasks should preserve context vars."""

    async def test_func() -> int:
        return ctxvar.get()

    receiver = get_receiver()

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={},
            args=[],
            kwargs={},
        ),
    )
    assert result.return_value == EXPECTED_CTX_VALUE


async def test_sync_decorator_on_async_function() -> None:
    broker = InMemoryBroker()
    wrapper_call = False

    def wrapper(f: Any) -> Any:
        @wraps(f)
        def wrapper_impl(*args: Any, **kwargs: Any) -> Any:
            nonlocal wrapper_call

            wrapper_call = True
            return f(*args, **kwargs)

        return wrapper_impl

    @broker.task
    @wrapper
    async def task_no_result() -> str:
        return "some value"

    task = await task_no_result.kiq()
    resp = await task.wait_result(timeout=1)

    assert resp.return_value == "some value"
    assert not broker._running_tasks
    assert wrapper_call is True


async def test_jitter_applied_to_semaphore() -> None:
    """Test that jitter is correctly applied to max_async_tasks semaphore."""
    max_async_tasks = 100
    max_async_tasks_jitter = 10

    # Test with jitter value of 0 (minimum)
    with unittest.mock.patch("random.randint", return_value=0):
        receiver = get_receiver(
            max_async_tasks=max_async_tasks,
            max_async_tasks_jitter=max_async_tasks_jitter,
        )
        assert receiver.sem is not None
        assert receiver.sem._value == max_async_tasks

    # Test with jitter value of 5 (middle)
    with unittest.mock.patch("random.randint", return_value=5):
        receiver = get_receiver(
            max_async_tasks=max_async_tasks,
            max_async_tasks_jitter=max_async_tasks_jitter,
        )
        assert receiver.sem is not None
        assert receiver.sem._value == max_async_tasks + 5

    # Test with jitter value of 10 (maximum)
    with unittest.mock.patch("random.randint", return_value=10):
        receiver = get_receiver(
            max_async_tasks=max_async_tasks,
            max_async_tasks_jitter=max_async_tasks_jitter,
        )
        assert receiver.sem is not None
        assert receiver.sem._value == max_async_tasks + 10


async def test_jitter_zero_no_randomization() -> None:
    """Test with zero jitter, semaphore value matches max_async_tasks."""
    max_async_tasks = 50

    receiver = get_receiver(
        max_async_tasks=max_async_tasks,
        max_async_tasks_jitter=0,
    )

    assert receiver.sem is not None
    assert receiver.sem._value == max_async_tasks


async def test_no_semaphore_without_max_async_tasks() -> None:
    """Test that semaphore is None when max_async_tasks is not set."""
    receiver = get_receiver(max_async_tasks=None)
    assert receiver.sem is None


async def test_prefetcher_does_not_pop_message_past_max_tasks() -> None:
    """Test not pulling a message without the intention of running it."""
    broker = AsyncQueueBroker()

    @broker.task
    async def noop() -> None:
        return None

    for _ in range(6):
        await noop.kiq()

    assert broker.queue.qsize() == 6

    receiver = Receiver(
        broker,
        executor=ThreadPoolExecutor(max_workers=1),
        max_async_tasks=1,
        max_tasks_to_execute=5,
    )

    await receiver.listen(asyncio.Event())

    assert broker.queue.qsize() == 1


async def test_prefetcher_recovers_from_transient_listen_error() -> None:
    """A transient error mid-prefetch must not kill the prefetcher."""

    class FlakyBroker(AsyncQueueBroker):
        def __init__(self) -> None:
            super().__init__()
            self.fail_once = True

        async def listen(self) -> AsyncGenerator[AckableMessage, None]:
            while True:
                data = await self.queue.get()
                if self.fail_once:
                    self.fail_once = False
                    self.queue.task_done()
                    raise RuntimeError("transient broker hiccup")
                yield AckableMessage(data=data, ack=self.queue.task_done)

    broker = FlakyBroker()
    ran = 0

    @broker.task
    async def collector() -> None:
        nonlocal ran
        ran += 1

    await collector.kiq()  # consumed by the transient error
    await collector.kiq()  # prefetcher recovering

    receiver = Receiver(
        broker,
        executor=ThreadPoolExecutor(max_workers=1),
        max_async_tasks=1,
        max_tasks_to_execute=1,
    )

    await asyncio.wait_for(receiver.listen(asyncio.Event()), timeout=5)

    assert ran == 1
