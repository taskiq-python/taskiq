import asyncio
from abc import ABC, abstractmethod
from time import time
from typing import Generic, Optional, TypeVar

from pydantic.generics import GenericModel

from taskiq.exceptions import TaskiqResultTimeoutError

_T = TypeVar("_T")  # noqa: WPS111
_ReturnType = TypeVar("_ReturnType")


class TaskiqResult(GenericModel, Generic[_ReturnType]):
    """Result of a remote task invocation."""

    is_err: bool
    log: Optional[str]
    return_value: _ReturnType
    execution_time: float


class AsyncResultBackend(ABC, Generic[_ReturnType]):
    """Async result backend."""

    async def startup(self) -> None:
        """Do something when starting broker."""

    async def shutdown(self) -> None:
        """Do something on shutdown."""

    def generate_task(self, task_id: str) -> "AsyncTaskiqTask[_ReturnType]":
        """
        Generates new task.

        This function creates new AsyncTaskiqTask
        that returned to client after calling kiq
        method.

        :param task_id: id of a task to save.
        :return: task object.
        """
        return AsyncTaskiqTask(task_id=task_id, result_backend=self)

    @abstractmethod
    async def set_result(self, task_id: str, result: TaskiqResult[_ReturnType]) -> None:
        """
        Saves result to the result backend.

        Result must be save so it can be accesed later
        by the calling side of the system.

        :param task_id: id of a task to save.
        :param result: result of execution.
        :return: nothing.
        """

    @abstractmethod
    async def is_result_ready(self, task_id: str) -> bool:
        """
        Checks that result of task is ready.

        :param task_id: task's id.
        :return: True if task is completed.
        """

    @abstractmethod
    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Gets result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :return: task's return value.
        """


class AsyncTaskiqTask(Generic[_ReturnType]):
    """AsyncTask for AsyncResultBackend."""

    def __init__(
        self,
        task_id: str,
        result_backend: AsyncResultBackend[_ReturnType],
    ) -> None:
        self.task_id = task_id
        self.result_backend = result_backend

    async def is_ready(self) -> bool:
        """
        Checks if task is completed.

        :return: True if task is completed.
        """
        return await self.result_backend.is_result_ready(self.task_id)

    async def get_result(self, with_logs: bool = False) -> TaskiqResult[_ReturnType]:
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
    ) -> TaskiqResult[_ReturnType]:
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
