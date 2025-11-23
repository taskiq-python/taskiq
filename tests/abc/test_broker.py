from collections.abc import AsyncGenerator

from taskiq.abc.broker import AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.message import BrokerMessage


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
