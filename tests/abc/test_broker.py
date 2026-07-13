from collections.abc import AsyncGenerator
from copy import copy

import pytest

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.events import TaskiqEvents
from taskiq.exceptions import SendTaskError
from taskiq.message import BrokerMessage, TaskiqMessage
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


class _RecordingSendBroker(_TestBroker):
    """Record transport dispatch and raise an optional send failure."""

    def __init__(
        self,
        events: list[str],
        kick_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self.events = events
        self.kick_error = kick_error

    async def kick(self, message: BrokerMessage) -> None:
        self.events.append("kick")
        if self.kick_error is not None:
            raise self.kick_error


class _RecordingSendMiddleware(TaskiqMiddleware):
    """Record client send hooks and raise an optional post-send failure."""

    def __init__(
        self,
        events: list[str],
        post_send_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self.events = events
        self.post_send_error = post_send_error

    def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        self.events.append("pre_send")
        return message

    def post_send(self, message: TaskiqMessage) -> None:
        self.events.append("post_send")
        if self.post_send_error is not None:
            raise self.post_send_error


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


async def test_kicker_preserves_external_broker_send_order() -> None:
    events: list[str] = []
    broker = _RecordingSendBroker(events)
    broker.with_middlewares(_RecordingSendMiddleware(events))

    @broker.task
    async def task() -> None:
        return None

    await task.kiq()

    assert events == ["pre_send", "kick", "post_send"]


async def test_kicker_wraps_external_broker_transport_error() -> None:
    events: list[str] = []
    kick_error = RuntimeError("transport failed")
    broker = _RecordingSendBroker(events, kick_error=kick_error)
    broker.with_middlewares(_RecordingSendMiddleware(events))

    @broker.task
    async def task() -> None:
        return None

    with pytest.raises(SendTaskError) as exc_info:
        await task.kiq()

    assert exc_info.value.__cause__ is kick_error
    assert events == ["pre_send", "kick"]


async def test_kicker_preserves_post_send_error_type() -> None:
    events: list[str] = []
    post_send_error = RuntimeError("post-send failed")
    broker = _RecordingSendBroker(events)
    broker.with_middlewares(
        _RecordingSendMiddleware(events, post_send_error=post_send_error),
    )

    @broker.task
    async def task() -> None:
        return None

    with pytest.raises(RuntimeError) as exc_info:
        await task.kiq()

    assert exc_info.value is post_send_error
    assert events == ["pre_send", "kick", "post_send"]


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
