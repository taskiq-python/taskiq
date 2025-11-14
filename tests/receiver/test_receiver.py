import asyncio
import contextvars
import random
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, ClassVar

import pytest
from taskiq_dependencies import Depends

from taskiq.abc.broker import AckableMessage, AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.exceptions import NoResultError, TaskiqResultTimeoutError
from taskiq.message import TaskiqMessage
from taskiq.receiver import Receiver
from taskiq.result import TaskiqResult
from tests.utils import AsyncQueueBroker


def get_receiver(
    broker: AsyncBroker | None = None,
    no_parse: bool = False,
    max_async_tasks: int | None = None,
) -> Receiver:
    """
    Returns receiver with custom broker and args.

    :param broker: broker, defaults to None
    :param no_parse: parameter to taskiq_args, defaults to False
    :param cli_args: Taskiq worker CLI arguments.
    :return: new receiver.
    """
    if broker is None:
        broker = InMemoryBroker()
    return Receiver(
        broker,
        executor=ThreadPoolExecutor(max_workers=10),
        validate_params=not no_parse,
        max_async_tasks=max_async_tasks,
    )


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
