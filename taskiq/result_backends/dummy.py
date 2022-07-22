from typing import Any, TypeVar

from taskiq.abc.result_backend import AsyncResultBackend

_ReturnType = TypeVar("_ReturnType")


class DummyResultBackend(AsyncResultBackend[_ReturnType]):
    """Default result backend, that does nothing."""

    async def set_result(self, task_id: str, result: Any) -> None:
        """
        Sets result.

        But actually it does nothing.

        :param task_id: current task id.
        :param result: result of execution.
        :return: nothing.
        """
        return await super().set_result(task_id, result)

    async def is_result_ready(self, _id: str) -> bool:
        """
        Checks whether task is completed.

        Result is always ready,
        since it doesn't care about tasks.

        :param _id: task's id.
        :return: true.
        """
        return True

    def get_result(self, _id: str) -> Any:
        """
        Returns None.

        This result doesn't care about passed parameters.

        :param _id: task's id.
        """
