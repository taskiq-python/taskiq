from typing import Any

import pytest
from mock import AsyncMock

from taskiq.exceptions import ResultIsReadyError, TaskiqResultTimeoutError
from taskiq.funcs import gather
from taskiq.task import AsyncTaskiqTask


@pytest.mark.anyio
async def test_gather() -> None:
    """Test successfull task gathering."""
    rb_mock = AsyncMock()
    rb_mock.is_result_ready.return_value = True
    rb_mock.get_result.return_value = 1

    task1: "AsyncTaskiqTask[Any]" = AsyncTaskiqTask(task_id="1", result_backend=rb_mock)
    task2: "AsyncTaskiqTask[Any]" = AsyncTaskiqTask(task_id="2", result_backend=rb_mock)

    assert await gather(task1, task2) == (1, 1)  # type: ignore


@pytest.mark.anyio
async def test_gather_timeout() -> None:
    """Tests how gather works if timeout is reached."""
    rb_mock = AsyncMock()
    rb_mock.is_result_ready.return_value = False

    task1: "AsyncTaskiqTask[Any]" = AsyncTaskiqTask(task_id="1", result_backend=rb_mock)
    task2: "AsyncTaskiqTask[Any]" = AsyncTaskiqTask(task_id="2", result_backend=rb_mock)

    with pytest.raises(TaskiqResultTimeoutError):
        await gather(task1, task2, timeout=0.4)


@pytest.mark.anyio
async def test_gather_result_backend_error() -> None:
    """Test how gather works if result backend doesn't work."""
    rb_mock = AsyncMock()
    rb_mock.is_result_ready.side_effect = Exception

    task1: "AsyncTaskiqTask[Any]" = AsyncTaskiqTask(task_id="1", result_backend=rb_mock)
    task2: "AsyncTaskiqTask[Any]" = AsyncTaskiqTask(task_id="2", result_backend=rb_mock)

    with pytest.raises(ResultIsReadyError):
        await gather(task1, task2)
