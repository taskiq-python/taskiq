import re

import pytest

from taskiq import InMemoryBroker
from taskiq.brokers.shared_broker import AsyncSharedBroker
from taskiq.exceptions import (
    SharedBrokerListenError,
    SharedBrokerSendTaskError,
    TaskBrokerMismatchError,
    UnknownTaskError,
)
from taskiq.message import BrokerMessage


def _broker_message(task_name: str) -> BrokerMessage:
    return BrokerMessage(
        task_id="task-id",
        task_name=task_name,
        message=b"{}",
        labels={},
    )


async def test_inmemory_broker_raises_unknown_task_error() -> None:
    broker = InMemoryBroker()

    with pytest.raises(
        UnknownTaskError,
        match=re.escape(
            "Cannot send unknown task to the queue, task name - missing.task",
        ),
    ):
        await broker.kick(_broker_message("missing.task"))


async def test_shared_broker_raises_send_task_error() -> None:
    broker = AsyncSharedBroker()

    with pytest.raises(
        SharedBrokerSendTaskError,
        match="You cannot use kiq directly on shared task",
    ):
        await broker.kick(_broker_message("any.task"))


async def test_shared_broker_raises_listen_error() -> None:
    broker = AsyncSharedBroker()

    with pytest.raises(SharedBrokerListenError, match="Shared broker cannot listen"):
        await broker.listen()


def test_registering_task_in_another_broker_raises_mismatch_error() -> None:
    first_broker = InMemoryBroker()
    second_broker = InMemoryBroker()

    @first_broker.task(task_name="test.task")
    async def test_task() -> None:
        return None

    with pytest.raises(
        TaskBrokerMismatchError,
        match="Task already has a different broker",
    ):
        second_broker._register_task(test_task.task_name, test_task)
