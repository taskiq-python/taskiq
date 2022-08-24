from typing import Any

import pytest

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.cli.async_task_runner import run_task
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult


@pytest.mark.anyio
async def test_run_task_successfull_async() -> None:
    """Tests that run_task can run async tasks."""

    async def test_func(param: int) -> int:
        return param

    result = await run_task(
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


@pytest.mark.anyio
async def test_run_task_successfull_sync() -> None:
    """Tests that run_task can run sync tasks."""

    def test_func(param: int) -> int:
        return param

    result = await run_task(
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


@pytest.mark.anyio
async def test_run_task_exception() -> None:
    """Tests that run_task can run sync tasks."""

    def test_func() -> None:
        raise ValueError()

    result = await run_task(
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


@pytest.mark.anyio
async def test_run_task_exception_middlewares() -> None:
    """Tests that run_task can run sync tasks."""

    class TestMiddleware(TaskiqMiddleware):
        found_exceptions = []

        def on_error(
            self,
            message: "TaskiqMessage",
            result: "TaskiqResult[Any]",
            exception: Exception,
        ) -> None:
            self.found_exceptions.append(exception)

    def test_func() -> None:
        raise ValueError()

    result = await run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={},
            args=[],
            kwargs={},
        ),
        middlewares=[TestMiddleware()],
    )
    assert result.return_value is None
    assert result.is_err
    assert len(TestMiddleware.found_exceptions) == 1
    assert TestMiddleware.found_exceptions[0].__class__ == ValueError
