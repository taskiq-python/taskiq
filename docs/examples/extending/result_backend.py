from typing import TypeVar

from taskiq import TaskiqResult
from taskiq.abc.result_backend import AsyncResultBackend

_ReturnType = TypeVar("_ReturnType")


class MyResultBackend(AsyncResultBackend[_ReturnType]):
    async def startup(self) -> None:
        """Do something when starting broker."""

    async def shutdown(self) -> None:
        """Do something on shutdown."""

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """
        Set result in your backend.

        :param task_id: current task id.
        :param result: result of execution.
        """

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Here you must retrieve result by id.

        Logs is a part of a result.
        Here we have a parameter whether you want to
        fetch result with logs or not, because logs
        can have a lot of info and sometimes it's critical
        to get only needed information.

        :param task_id: id of a task.
        :param with_logs: whether to fetch logs.
        :return: result.
        """
        return ...  # type: ignore

    async def is_result_ready(
        self,
        task_id: str,
    ) -> bool:
        """
        Check if result exists.

        This function must check whether result
        is available in your result backend
        without fetching the result.

        :param task_id: id of a task.
        :return: True if result is ready.
        """
        return ...  # type: ignore
