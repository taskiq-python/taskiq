from typing import Any, TypeVar

from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.result import TaskiqResult

_ReturnType = TypeVar("_ReturnType")


class DummyResultBackend(AsyncResultBackend[_ReturnType]):  # pragma: no cover
    """Default result backend, that does nothing."""

    async def set_result(self, task_id: str, result: Any) -> None:
        """
        Sets result.

        But actually it does nothing.

        :param task_id: current task id.
        :param result: result of execution.
        """

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Checks whether task is completed.

        Result is always ready,
        since it doesn't care about tasks.

        :param task_id: task's id.
        :return: true.
        """
        return True

    async def get_result(self, task_id: str, with_logs: bool = False) -> Any:
        """
        Returns None.

        This result doesn't care about passed parameters.

        :param task_id: task's id.
        :param with_logs: wether to fetch logs.
        :returns: TaskiqResult.
        """
        return TaskiqResult(
            is_err=False,
            log=None,
            return_value=None,
            execution_time=0,
        )
