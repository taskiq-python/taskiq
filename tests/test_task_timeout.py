from types import ModuleType
from unittest.mock import AsyncMock, Mock

import pytest

import taskiq.funcs as funcs_module
import taskiq.task as task_module
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.exceptions import TaskiqResultTimeoutError
from taskiq.result import TaskiqResult
from taskiq.task import AsyncTaskiqTask


async def wait_for_result(
    task: AsyncTaskiqTask[int],
    module: ModuleType,
    timeout: float,
) -> TaskiqResult[int]:
    """Exercise either public result-waiting API."""
    if module is funcs_module:
        return (await funcs_module.gather(task, timeout=timeout, periodicity=0))[0]
    return await task.wait_result(timeout=timeout, check_interval=0)


@pytest.mark.parametrize("module", [task_module, funcs_module])
async def test_result_timeout_survives_backward_wall_clock(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
) -> None:
    """A backwards wall-clock jump must not postpone the timeout."""
    # Keep the old clock patched so this also reproduces on the unfixed source.
    monkeypatch.setattr(
        module,
        "time",
        Mock(side_effect=[100, 99, 98, 97]),
        raising=False,
    )
    monkeypatch.setattr(
        module,
        "monotonic",
        Mock(side_effect=[0, 0.5, 1.1]),
        raising=False,
    )
    backend = AsyncMock(spec=AsyncResultBackend)
    backend.is_result_ready.side_effect = [False, False, True]
    backend.get_result.return_value = TaskiqResult(
        is_err=False,
        return_value=42,
        execution_time=0,
    )
    task = AsyncTaskiqTask[int]("task", backend)

    with pytest.raises(TaskiqResultTimeoutError):
        await wait_for_result(task, module, timeout=1)

    assert backend.is_result_ready.await_count == (1 if module is funcs_module else 2)
    backend.get_result.assert_not_awaited()


@pytest.mark.parametrize("module", [task_module, funcs_module])
async def test_forward_wall_clock_does_not_expire_result_wait(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
) -> None:
    """A forward wall-clock jump must not end an otherwise timely wait."""
    monkeypatch.setattr(module, "time", Mock(side_effect=[100, 10000]), raising=False)
    monkeypatch.setattr(
        module,
        "monotonic",
        Mock(side_effect=[0, 0.25, 0.5]),
        raising=False,
    )
    backend = AsyncMock(spec=AsyncResultBackend)
    backend.is_result_ready.side_effect = [False, True]
    result = TaskiqResult(is_err=False, return_value=42, execution_time=0)
    backend.get_result.return_value = result
    task = AsyncTaskiqTask[int]("task", backend)

    assert await wait_for_result(task, module, timeout=1) is result


@pytest.mark.parametrize("module", [task_module, funcs_module])
@pytest.mark.parametrize("timeout", [-1, 0])
async def test_result_wait_keeps_unlimited_timeout(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    timeout: float,
) -> None:
    """Nonpositive timeouts keep polling until the result is available."""
    monkeypatch.setattr(
        module,
        "time",
        Mock(side_effect=[100, 99, 98, 97]),
        raising=False,
    )
    monkeypatch.setattr(
        module,
        "monotonic",
        Mock(side_effect=[0, 100, 200]),
        raising=False,
    )
    backend = AsyncMock(spec=AsyncResultBackend)
    backend.is_result_ready.side_effect = [False, False, True]
    result = TaskiqResult(is_err=False, return_value=42, execution_time=0)
    backend.get_result.return_value = result
    task = AsyncTaskiqTask[int]("task", backend)

    assert await wait_for_result(task, module, timeout=timeout) is result
    assert backend.is_result_ready.await_count == 3


@pytest.mark.parametrize("module", [task_module, funcs_module])
async def test_result_wait_keeps_strict_timeout_boundary(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
) -> None:
    """Elapsed time equal to the timeout does not expire the wait."""
    monkeypatch.setattr(module, "time", Mock(side_effect=[0, 1]), raising=False)
    monkeypatch.setattr(module, "monotonic", Mock(side_effect=[0, 1]), raising=False)
    backend = AsyncMock(spec=AsyncResultBackend)
    backend.is_result_ready.side_effect = (
        [False, True] if module is task_module else [True]
    )
    result = TaskiqResult(is_err=False, return_value=42, execution_time=0)
    backend.get_result.return_value = result
    task = AsyncTaskiqTask[int]("task", backend)

    assert await wait_for_result(task, module, timeout=1) is result
