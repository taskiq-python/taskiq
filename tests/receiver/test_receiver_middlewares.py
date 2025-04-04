from typing import Any, ClassVar, List

import pytest

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult


@pytest.mark.anyio
async def test_run_middleware_on_error() -> None:
    """Tests that run_task can run sync tasks."""

    class _TestMiddleware(TaskiqMiddleware):
        found_exceptions: ClassVar[List[BaseException]] = []

        def on_error(
            self,
            message: "TaskiqMessage",
            result: "TaskiqResult[Any]",
            exception: BaseException,
        ) -> None:
            self.found_exceptions.append(exception)

    broker = InMemoryBroker().with_middlewares(_TestMiddleware())

    @broker.task
    def test_func() -> None:
        raise ValueError

    task = await test_func.kiq()

    result = await task.wait_result()
    assert result.return_value is None
    assert result.is_err
    assert len(_TestMiddleware.found_exceptions) == 1
    assert _TestMiddleware.found_exceptions[0].__class__ is ValueError


@pytest.mark.anyio
async def test_run_middleware_on_success() -> None:
    """Tests that run_task can run sync tasks."""
    ran_pre_send = False
    ran_post_send = False
    ran_pre_execute = False
    ran_post_execute = False

    class _TestMiddleware(TaskiqMiddleware):
        async def pre_send(self, message: "TaskiqMessage") -> TaskiqMessage:
            nonlocal ran_pre_send
            ran_pre_send = True
            return message

        async def post_send(self, message: "TaskiqMessage") -> None:
            nonlocal ran_post_send
            ran_post_send = True

        async def pre_execute(self, message: "TaskiqMessage") -> TaskiqMessage:
            nonlocal ran_pre_execute
            ran_pre_execute = True
            return message

        async def post_execute(
            self,
            message: "TaskiqMessage",
            result: "TaskiqResult[Any]",
        ) -> None:
            nonlocal ran_post_execute
            ran_post_execute = True

    broker = InMemoryBroker().with_middlewares(_TestMiddleware())

    @broker.task
    def test_func() -> None:
        return None

    task = await test_func.kiq()

    result = await task.wait_result()
    assert result.return_value is None
    assert not result.is_err
    assert ran_pre_send and ran_post_send and ran_pre_execute and ran_post_execute
