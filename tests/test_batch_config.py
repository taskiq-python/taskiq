import pytest

from taskiq import InMemoryBroker
from taskiq.exceptions import TaskiqBatchConfigError


def test_batch_without_size_or_timeout_raises() -> None:
    broker = InMemoryBroker()
    with pytest.raises(TaskiqBatchConfigError):

        @broker.task(batch=True)
        async def my_task(items: list[int]) -> None: ...


def test_batch_size_must_be_positive() -> None:
    broker = InMemoryBroker()
    with pytest.raises(TaskiqBatchConfigError):

        @broker.task(batch=True, batch_size=0)
        async def my_task(items: list[int]) -> None: ...


def test_batch_timeout_must_be_positive() -> None:
    broker = InMemoryBroker()
    with pytest.raises(TaskiqBatchConfigError):

        @broker.task(batch=True, batch_timeout=0)
        async def my_task(items: list[int]) -> None: ...


def test_valid_batch_config_ok() -> None:
    broker = InMemoryBroker()

    @broker.task(batch=True, batch_size=10, batch_timeout=2)
    async def my_task(items: list[int]) -> None: ...

    assert my_task.labels["batch"] is True
    assert my_task.labels["batch_size"] == 10
    assert my_task.labels["batch_timeout"] == 2
