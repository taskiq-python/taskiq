from collections.abc import AsyncGenerator
from concurrent.futures import ProcessPoolExecutor
from copy import copy
from types import MethodType

import pytest

from taskiq import InMemoryBroker
from taskiq.abc.broker import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.events import TaskiqEvents
from taskiq.message import BrokerMessage
from taskiq.state import TaskiqState


class _TestBroker(AsyncBroker):
    """Broker for testing purpose."""

    async def kick(self, message: BrokerMessage) -> None:
        """
        This method is used to send messages.

        But in this case it just throws messages away.

        :param message: message to lost.
        """

    async def listen(self) -> AsyncGenerator[BrokerMessage, None]:  # type: ignore
        """
        This method is not implemented.

        :param callback: callback that is never called.
        """


_process_pool_broker = _TestBroker()


@_process_pool_broker.task(task_name="process_pool_sync_add")
def process_pool_sync_add(value: int) -> int:
    """Module-level sync task used by ProcessPoolExecutor regression test."""
    return value + 1


def test_decorator_success() -> None:
    """Test that decoration without parameters works."""
    tbrok = _TestBroker()

    @tbrok.task
    async def test_func() -> None:
        """Some test function."""

    assert isinstance(test_func, AsyncTaskiqDecoratedTask)


def test_decorator_with_name_success() -> None:
    """Test that task_name is successfully set."""
    tbrok = _TestBroker()

    @tbrok.task(task_name="my_task")
    async def test_func() -> None:
        """Some test function."""

    assert isinstance(test_func, AsyncTaskiqDecoratedTask)
    assert test_func.task_name == "my_task"


def test_decorator_with_labels_success() -> None:
    """Tests that labels are assigned for task as is."""
    tbrok = _TestBroker()

    @tbrok.task(label1=1, label2=2)
    async def test_func() -> None:
        """Some test function."""

    assert isinstance(test_func, AsyncTaskiqDecoratedTask)
    assert test_func.labels == {
        "label1": 1,
        "label2": 2,
    }


def test_kicker_labels_modification() -> None:
    """Test that using kicker.with_labels doesn't modify task's labels globally."""
    broker = _TestBroker()

    @broker.task(test_lb="one")
    async def test_task() -> None: ...

    old_labels = copy(test_task.labels)
    test_kicker = test_task.kicker().with_labels(another_label="test")
    assert "another_label" in test_kicker.labels

    assert test_task.labels == old_labels


def test_register_task_accepts_bound_method() -> None:
    """Bound methods must register without mutating method.__name__."""
    broker = _TestBroker()

    class Counter:
        def increment(self, value: int) -> int:
            return value + 1

    instance = Counter()
    task = broker.register_task(instance.increment)

    assert isinstance(task, AsyncTaskiqDecoratedTask)
    assert isinstance(task.original_func, MethodType)
    assert task.original_func.__self__ is instance
    assert task.task_name == f"{instance.increment.__module__}:increment"
    assert broker.find_task(task.task_name) is task


@pytest.mark.anyio
async def test_bound_method_task_executes_with_self() -> None:
    """Registered bound method must keep instance state across execution."""
    broker = InMemoryBroker(await_inplace=True)

    class Counter:
        def __init__(self) -> None:
            self.total = 0

        def add(self, value: int) -> int:
            self.total += value
            return self.total

    instance = Counter()
    task = broker.register_task(instance.add, task_name="bound.add")

    kicked = await task.kiq(5)
    result = await kicked.wait_result()
    assert result.return_value == 5
    assert instance.total == 5


def test_process_pool_runs_module_level_decorated_sync_function() -> None:
    """ProcessPoolExecutor must be able to run renamed original sync functions."""
    with ProcessPoolExecutor(max_workers=1) as executor:
        future = executor.submit(process_pool_sync_add.original_func, 41)
        assert future.result(timeout=5) == 42


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("is_worker_process", "startup", "shutdown"),
    [
        (True, TaskiqEvents.WORKER_STARTUP, TaskiqEvents.WORKER_SHUTDOWN),
        (False, TaskiqEvents.CLIENT_STARTUP, TaskiqEvents.CLIENT_SHUTDOWN),
    ],
)
async def test_async_context_manager_enter(
    *,
    is_worker_process: bool,
    startup: TaskiqEvents,
    shutdown: TaskiqEvents,
) -> None:
    """Test that `__aenter__` and `__aexit__` calls work."""
    broker = _TestBroker()
    broker.is_worker_process = is_worker_process
    startup_called = False
    shutdown_called = False

    @broker.on_event(startup)
    async def track_startup(state: TaskiqState) -> None:
        nonlocal startup_called
        startup_called = True

    @broker.on_event(shutdown)
    async def track_shutdown(state: TaskiqState) -> None:
        nonlocal shutdown_called
        shutdown_called = True

    async with broker as ctx:
        assert ctx is None
        assert startup_called is True
        assert shutdown_called is False

    assert shutdown_called is True


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("is_worker_process", "startup", "shutdown"),
    [
        (True, TaskiqEvents.WORKER_STARTUP, TaskiqEvents.WORKER_SHUTDOWN),
        (False, TaskiqEvents.CLIENT_STARTUP, TaskiqEvents.CLIENT_SHUTDOWN),
    ],
)
async def test_async_context_manager_exit_on_exception(
    *,
    is_worker_process: bool,
    startup: TaskiqEvents,
    shutdown: TaskiqEvents,
) -> None:
    """Test that __aexit__ calls shutdown even if exception is raised."""
    broker = _TestBroker()
    broker.is_worker_process = is_worker_process
    startup_called = False
    shutdown_called = False

    @broker.on_event(startup)
    async def track_startup(state: TaskiqState) -> None:
        nonlocal startup_called
        startup_called = True

    @broker.on_event(shutdown)
    async def track_shutdown(state: TaskiqState) -> None:
        nonlocal shutdown_called
        shutdown_called = True

    with pytest.raises(ValueError, match="Test exception"):
        async with broker:
            assert startup_called is True
            assert shutdown_called is False
            raise ValueError("Test exception")

    assert shutdown_called is True
