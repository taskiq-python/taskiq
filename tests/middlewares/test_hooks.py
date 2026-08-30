import asyncio

import pytest

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.exceptions import SendTaskError
from taskiq.message import BrokerMessage, TaskiqMessage


@pytest.mark.anyio
async def test_on_send_error() -> None:
    caught = []

    class _TestMiddleware(TaskiqMiddleware):
        def on_send_error(
            self,
            message: "TaskiqMessage",
            broker_message: "BrokerMessage",
            exception: BaseException,
        ) -> bool:
            caught.append(1)
            return True

    broker = InMemoryBroker().with_middlewares(_TestMiddleware())

    broker.kick = lambda *args, **kwargs: (_ for _ in ()).throw(Exception("test"))  # type: ignore

    await broker.startup()
    await broker.task(lambda: None).kiq()
    await broker.shutdown()

    assert caught == [1]


@pytest.mark.anyio
async def test_on_send_error_raise() -> None:
    caught = []

    class _TestMiddleware(TaskiqMiddleware):
        def on_send_error(
            self,
            message: "TaskiqMessage",
            broker_message: "BrokerMessage",
            exception: BaseException,
        ) -> None:
            caught.append(0)

    broker = InMemoryBroker().with_middlewares(_TestMiddleware())

    broker.kick = lambda *args, **kwargs: (_ for _ in ()).throw(Exception("test"))  # type: ignore

    await broker.startup()

    with pytest.raises(SendTaskError):
        await broker.task(lambda: None).kiq()

    await broker.shutdown()

    assert caught == [0]


@pytest.mark.anyio
async def test_on_send_error_inverted() -> None:
    caught = []

    class _TestMiddleware1(TaskiqMiddleware):
        def on_send_error(
            self,
            message: "TaskiqMessage",
            broker_message: "BrokerMessage",
            exception: BaseException,
        ) -> bool:
            caught.append(1)
            return True

    class _TestMiddleware2(TaskiqMiddleware):
        async def on_send_error(
            self,
            message: "TaskiqMessage",
            broker_message: "BrokerMessage",
            exception: BaseException,
        ) -> bool:
            await asyncio.sleep(0)
            caught.append(2)
            return True

    broker = InMemoryBroker().with_middlewares(_TestMiddleware1(), _TestMiddleware2())

    broker.kick = lambda *args, **kwargs: (_ for _ in ()).throw(Exception("test"))  # type: ignore

    await broker.startup()
    await broker.task(lambda: None).kiq()
    await broker.shutdown()

    assert caught == [2, 1]
