import asyncio
from time import time
from typing import TYPE_CHECKING, Generic

from taskiq.exceptions import TaskiqResultTimeoutError
from taskiq.types_helpers import ReturnType_

if TYPE_CHECKING:
    from taskiq.abc.result_backend import AsyncResultBackend
    from taskiq.result import TaskiqResult


class AsyncTaskiqTask(Generic[ReturnType_]):
    """AsyncTask for AsyncResultBackend."""

    def __init__(
        self,
        task_id: str,
        result_backend: "AsyncResultBackend[ReturnType_]",
    ) -> None:
        self.task_id = task_id
        self.result_backend = result_backend

    async def is_ready(self) -> bool:
        """
        Checks if task is completed.

        :return: True if task is completed.
        """
        return await self.result_backend.is_result_ready(self.task_id)

    async def get_result(self, with_logs: bool = False) -> "TaskiqResult[ReturnType_]":
        """
        Get result of a task from result backend.

        :param with_logs: whether you want to fetch logs from worker.
        :return: task's return value.
        """
        return await self.result_backend.get_result(self.task_id, with_logs=with_logs)

    async def wait_result(
        self,
        check_interval: float = 1.0,
        timeout: float = 5.0,
        with_logs: bool = False,
    ) -> "TaskiqResult[ReturnType_]":
        """
        Waits until result is ready.

        This method just checks whether the task is
        ready. And if it is it returns the result.

        It may throw TaskiqResultTimeoutError if
        task didn't became ready in provided
        period of time.

        :param check_interval: How often checks are performed.
        :param timeout: timeout for the result.
        :param with_logs: whether you want to fetch logs from worker.
        :raises TaskiqResultTimeoutError: if task didn't
            become ready in provided period of time.
        :return: task's return value.
        """
        start_time = time()
        while not await self.is_ready():
            await asyncio.sleep(check_interval)
            if time() - start_time > timeout:
                raise TaskiqResultTimeoutError()
        return await self.get_result(with_logs=with_logs)
