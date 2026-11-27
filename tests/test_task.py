import asyncio
import uuid
from typing import TypeVar

import pytest
from pydantic import BaseModel

from taskiq import serializers
from taskiq.abc import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.exceptions import (
    ResultGetError,
    ResultIsReadyError,
    TaskiqResultTimeoutError,
)
from taskiq.result import TaskiqResult
from taskiq.task import AsyncTaskiqTask

_ReturnType = TypeVar("_ReturnType")


class SerializingBackend(AsyncResultBackend[_ReturnType]):
    def __init__(self, serializer: TaskiqSerializer) -> None:
        self._serializer = serializer
        self._results: dict[str, bytes] = {}

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],  # type: ignore
    ) -> None:
        """Set result with dumping."""
        self._results[task_id] = self._serializer.dumpb(result.model_dump(mode="json"))

    async def is_result_ready(self, task_id: str) -> bool:
        """Check if result is ready."""
        return task_id in self._results

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """Get result with loading."""
        data = self._results[task_id]
        return TaskiqResult.model_validate(self._serializer.loadb(data))


@pytest.mark.parametrize(
    "serializer",
    [
        serializers.MSGPackSerializer(),
        serializers.CBORSerializer(),
        serializers.PickleSerializer(),
        serializers.JSONSerializer(),
    ],
)
async def test_res_parsing_success(serializer: TaskiqSerializer) -> None:
    class MyResult(BaseModel):
        name: str
        age: int

    res = MyResult(name="test", age=10)
    res_back: AsyncResultBackend[MyResult] = SerializingBackend(serializer)
    test_id = str(uuid.uuid4())
    await res_back.set_result(
        test_id,
        TaskiqResult(
            is_err=False,
            return_value=res,
            execution_time=0.0,
        ),
    )
    sent_task = AsyncTaskiqTask(test_id, res_back, MyResult)
    parsed = await sent_task.wait_result()
    assert isinstance(parsed.return_value, MyResult)


class FlakyResultBackend(AsyncResultBackend[int]):
    """Result backend that fails the first N calls of each kind."""

    def __init__(
        self,
        ready_failures: int = 0,
        get_failures: int = 0,
        ready_after: int = 0,
    ) -> None:
        self.ready_failures = ready_failures
        self.get_failures = get_failures
        self.ready_after = ready_after
        self.ready_calls = 0
        self.get_calls = 0

    async def set_result(self, task_id: str, result: TaskiqResult[int]) -> None:
        """Not used in these tests."""

    async def is_result_ready(self, task_id: str) -> bool:
        """Fail `ready_failures` times, then report ready after more polls."""
        self.ready_calls += 1
        if self.ready_calls <= self.ready_failures:
            raise ConnectionError("result backend is down")
        return self.ready_calls > self.ready_failures + self.ready_after

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[int]:
        """Fail `get_failures` times, then return a result."""
        self.get_calls += 1
        if self.get_calls <= self.get_failures:
            raise ConnectionError("result backend is down")
        return TaskiqResult(is_err=False, return_value=42, execution_time=0.0)


async def test_wait_result_fails_fast_on_backend_error_by_default() -> None:
    backend = FlakyResultBackend(ready_failures=1)
    task = AsyncTaskiqTask("task-id", backend)

    with pytest.raises(ResultIsReadyError):
        await task.wait_result(check_interval=0.001)

    assert backend.ready_calls == 1


async def test_wait_result_retries_is_ready_backend_error() -> None:
    backend = FlakyResultBackend(ready_failures=3, ready_after=2)
    task = AsyncTaskiqTask("task-id", backend)

    result = await task.wait_result(
        check_interval=0.001,
        timeout=5,
        retry_on_backend_error=True,
    )

    assert result.return_value == 42
    assert backend.ready_calls == 6


async def test_wait_result_timeout_keeps_last_backend_error_as_cause() -> None:
    backend = FlakyResultBackend(ready_failures=10**9)
    task = AsyncTaskiqTask("task-id", backend)

    with pytest.raises(TaskiqResultTimeoutError) as exc_info:
        await task.wait_result(
            check_interval=0.001,
            timeout=0.05,
            retry_on_backend_error=True,
        )

    assert isinstance(exc_info.value.__cause__, ResultIsReadyError)
    assert backend.ready_calls > 1


async def test_wait_result_backs_off_while_backend_is_failing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)
        await real_sleep(0)

    monkeypatch.setattr("taskiq.task.asyncio.sleep", fake_sleep)
    backend = FlakyResultBackend(ready_failures=4, ready_after=1)
    task = AsyncTaskiqTask("task-id", backend)

    await task.wait_result(check_interval=0.1, retry_on_backend_error=True)

    # Four failures back off exponentially, then one regular "not ready" poll
    # goes back to the configured check interval.
    assert delays == pytest.approx([0.2, 0.4, 0.8, 1.6, 0.1])


async def test_wait_result_backoff_does_not_overshoot_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delays: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)
        await real_sleep(0.02)

    monkeypatch.setattr("taskiq.task.asyncio.sleep", fake_sleep)
    backend = FlakyResultBackend(ready_failures=10**9)
    task = AsyncTaskiqTask("task-id", backend)

    with pytest.raises(TaskiqResultTimeoutError):
        await task.wait_result(
            check_interval=1,
            timeout=0.1,
            retry_on_backend_error=True,
        )

    assert all(delay <= 0.1 for delay in delays)


async def test_wait_result_fails_fast_on_get_result_error_by_default() -> None:
    backend = FlakyResultBackend(get_failures=1)
    task = AsyncTaskiqTask("task-id", backend)

    with pytest.raises(ResultGetError):
        await task.wait_result(check_interval=0.001)

    assert backend.get_calls == 1


async def test_wait_result_retries_get_result_backend_error() -> None:
    backend = FlakyResultBackend(get_failures=2)
    task = AsyncTaskiqTask("task-id", backend)

    result = await task.wait_result(
        check_interval=0.001,
        timeout=5,
        retry_on_backend_error=True,
    )

    assert result.return_value == 42
    assert backend.get_calls == 3


async def test_wait_result_timeout_after_get_result_errors() -> None:
    backend = FlakyResultBackend(get_failures=10**9)
    task = AsyncTaskiqTask("task-id", backend)

    with pytest.raises(TaskiqResultTimeoutError) as exc_info:
        await task.wait_result(
            check_interval=0.001,
            timeout=0.05,
            retry_on_backend_error=True,
        )

    assert isinstance(exc_info.value.__cause__, ResultGetError)
