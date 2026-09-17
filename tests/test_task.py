import uuid
from typing import TypeVar

import pytest
from pydantic import BaseModel

from taskiq import serializers
from taskiq.abc import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.exceptions import ResultIsReadyError, TaskiqResultTimeoutError
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
        self._results[task_id] = self._serializer.dumpb(model_dump(result))

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
        return model_validate(TaskiqResult, self._serializer.loadb(data))


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


class ScriptedBackend(AsyncResultBackend[str]):
    """Backend whose readiness checks follow a fixed script.

    Each item of the script is either a value to return or an exception
    to raise. The last item repeats once the script runs out.
    """

    def __init__(self, script: list[bool | Exception]) -> None:
        self.script = script
        self.polls = 0

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[str],
    ) -> None:
        """Results are produced by the script, so nothing is stored."""

    async def is_result_ready(self, task_id: str) -> bool:
        """Perform the next scripted readiness check."""
        step = self.script[min(self.polls, len(self.script) - 1)]
        self.polls += 1
        if isinstance(step, Exception):
            raise step
        return step

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[str]:
        """Return a fixed successful result."""
        return TaskiqResult(is_err=False, return_value="done", execution_time=0.0)


async def test_wait_result_survives_transient_error() -> None:
    backend = ScriptedBackend([ConnectionError("connection reset"), True])
    task: AsyncTaskiqTask[str] = AsyncTaskiqTask("task-id", backend)

    result = await task.wait_result(check_interval=0.0)

    assert result.return_value == "done"
    assert backend.polls == 2


async def test_wait_result_raises_if_backend_never_recovers() -> None:
    backend = ScriptedBackend([ConnectionError("backend is down")])
    task: AsyncTaskiqTask[str] = AsyncTaskiqTask("task-id", backend)

    with pytest.raises(ResultIsReadyError):
        await task.wait_result(check_interval=0.0, max_poll_failures=2)

    # Two tolerated failures, then the third one is fatal.
    assert backend.polls == 3


async def test_wait_result_counts_failures_consecutively() -> None:
    backend = ScriptedBackend(
        [
            ConnectionError("connection reset"),
            False,
            ConnectionError("connection reset"),
            False,
            ConnectionError("connection reset"),
            True,
        ],
    )
    task: AsyncTaskiqTask[str] = AsyncTaskiqTask("task-id", backend)

    # Three failures in total, but never two in a row.
    result = await task.wait_result(check_interval=0.0, max_poll_failures=1)

    assert result.return_value == "done"
    assert backend.polls == 6


async def test_wait_result_no_retries_by_configuration() -> None:
    backend = ScriptedBackend([ConnectionError("connection reset"), True])
    task: AsyncTaskiqTask[str] = AsyncTaskiqTask("task-id", backend)

    with pytest.raises(ResultIsReadyError):
        await task.wait_result(check_interval=0.0, max_poll_failures=0)

    assert backend.polls == 1


async def test_wait_result_timeout_while_retrying() -> None:
    backend = ScriptedBackend([ConnectionError("backend is down")])
    task: AsyncTaskiqTask[str] = AsyncTaskiqTask("task-id", backend)

    with pytest.raises(TaskiqResultTimeoutError):
        await task.wait_result(
            check_interval=0.05,
            timeout=0.1,
            max_poll_failures=1000,
        )
