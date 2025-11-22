import asyncio
from typing import Any

import pytest

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult


@pytest.mark.anyio
async def test_set_broker() -> None:

    class _TestMiddleware(TaskiqMiddleware):
        def set_broker(self, broker: "AsyncBroker") -> None:
            super().set_broker(broker)
            self.test_value = 1

    middleware = _TestMiddleware()
    broker = InMemoryBroker().with_middlewares(middleware)

    assert middleware is broker.middlewares[0]
    assert middleware.test_value == 1


@pytest.mark.anyio
async def test_startup_shutdown_in_pair() -> None:
    test_list = []

    class _TestMiddleware1(TaskiqMiddleware):
        def startup(self) -> None:
            test_list.append("1up")

        def shutdown(self) -> None:
            test_list.append("1down")

    class _TestMiddleware2(TaskiqMiddleware):
        async def startup(self) -> None:
            await asyncio.sleep(0)
            test_list.append("2up")

        async def shutdown(self) -> None:
            await asyncio.sleep(0)
            test_list.append("2down")

    broker = InMemoryBroker().with_middlewares(_TestMiddleware1(), _TestMiddleware2())

    await broker.startup()
    await broker.shutdown()

    assert test_list == ["1up", "2up", "2down", "1down"]


@pytest.mark.anyio
async def test_pre_post_send_in_pair() -> None:
    test_list = []

    class _TestMiddleware1(TaskiqMiddleware):
        def pre_send(self, message: "TaskiqMessage") -> "TaskiqMessage":
            test_list.append("1pre")
            return message

        def post_send(self, message: "TaskiqMessage") -> None:
            test_list.append("1post")

    class _TestMiddleware2(TaskiqMiddleware):
        def pre_send(self, message: "TaskiqMessage") -> "TaskiqMessage":
            test_list.append("2pre")
            return message

        def post_send(self, message: "TaskiqMessage") -> None:
            test_list.append("2post")

    broker = InMemoryBroker().with_middlewares(_TestMiddleware1(), _TestMiddleware2())

    await broker.startup()
    await broker.task(lambda: None).kiq()
    await broker.shutdown()

    assert test_list == ["1pre", "2pre", "2post", "1post"]


@pytest.mark.anyio
async def test_pre_post_execute_in_pair() -> None:
    test_list = []

    class _TestMiddleware1(TaskiqMiddleware):
        def pre_execute(self, message: "TaskiqMessage") -> "TaskiqMessage":
            test_list.append("1pre")
            return message

        def post_execute(self, message: "TaskiqMessage", result: "TaskiqResult[Any]") -> None:
            test_list.append("1post")

    class _TestMiddleware2(TaskiqMiddleware):
        def pre_execute(self, message: "TaskiqMessage") -> "TaskiqMessage":
            test_list.append("2pre")
            return message

        def post_execute(self, message: "TaskiqMessage", result: "TaskiqResult[Any]") -> None:
            test_list.append("2post")

    broker = InMemoryBroker().with_middlewares(_TestMiddleware1(), _TestMiddleware2())

    await broker.startup()
    task = await broker.task(lambda: 1).kiq()
    await task.wait_result(timeout=2)
    await broker.shutdown()

    assert test_list == ["1pre", "2pre", "2post", "1post"]


@pytest.mark.anyio
async def test_post_save_inverted() -> None:
    test_list = []

    class _TestMiddleware1(TaskiqMiddleware):
        def post_save(self, message: "TaskiqMessage", result: "TaskiqResult[Any]") -> None:
            test_list.append("1save")

    class _TestMiddleware2(TaskiqMiddleware):
        def post_save(self, message: "TaskiqMessage", result: "TaskiqResult[Any]") -> None:
            test_list.append("2save")

    broker = InMemoryBroker().with_middlewares(_TestMiddleware1(), _TestMiddleware2())

    await broker.startup()
    task = await broker.task(lambda: 1).kiq()
    await task.wait_result(timeout=2)
    await broker.shutdown()

    assert test_list == ["2save", "1save"]


@pytest.mark.anyio
async def test_post_on_error_inverted() -> None:
    test_list = []

    class _TestMiddleware1(TaskiqMiddleware):
        def on_error(self, message: "TaskiqMessage", result: "TaskiqResult[Any]", exception: BaseException) -> None:
            test_list.append("1error")

    class _TestMiddleware2(TaskiqMiddleware):
        def on_error(self, message: "TaskiqMessage", result: "TaskiqResult[Any]", exception: BaseException) -> None:
            test_list.append("2error")

    broker = InMemoryBroker().with_middlewares(_TestMiddleware1(), _TestMiddleware2())

    await broker.startup()
    task = await broker.task(lambda: (_ for _ in ()).throw(Exception("test"))).kiq()
    await task.wait_result(timeout=2)
    await broker.shutdown()

    assert test_list == ["2error", "1error"]
