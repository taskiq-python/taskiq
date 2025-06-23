import uuid
from typing import Dict, TypeVar

import pytest
from pydantic import BaseModel

from taskiq import serializers
from taskiq.abc import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.result import TaskiqResult
from taskiq.task import AsyncTaskiqTask

_ReturnType = TypeVar("_ReturnType")


class SerializingBackend(AsyncResultBackend[_ReturnType]):
    def __init__(self, serializer: TaskiqSerializer) -> None:
        self._serializer = serializer
        self._results: Dict[str, bytes] = {}

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
@pytest.mark.anyio
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
