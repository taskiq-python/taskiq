import asyncio
from abc import ABC, abstractmethod
from time import time
from typing import TYPE_CHECKING, Any, Coroutine, Generic, Optional, Union

from typing_extensions import TypeVar

from taskiq.exceptions import (
    ResultGetError,
    ResultIsReadyError,
    TaskiqResultTimeoutError,
)

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.result_backend import AsyncResultBackend
    from taskiq.depends.progress_tracker import TaskProgress
    from taskiq.result import TaskiqResult

_ReturnType = TypeVar("_ReturnType")


class _Task(ABC, Generic[_ReturnType]):
    """TaskiqTask interface."""

    @abstractmethod
    def is_ready(self) -> Union[bool, Coroutine[Any, Any, bool]]:
        """
        Method to check wether result is ready.

        :return: True if result is ready.
        """

    @abstractmethod
    def get_result(
        self,
        with_logs: bool = False,
    ) -> Union[
        "TaskiqResult[_ReturnType]",
        Coroutine[Any, Any, "TaskiqResult[_ReturnType]"],
    ]:
        """
        Get actual execution result.

        :param with_logs: wether you want to fetch logs.
        :return: TaskiqResult.
        """

    @abstractmethod
    def wait_result(
        self,
        check_interval: float = 0.2,
        timeout: float = -1.0,
        with_logs: bool = False,
    ) -> Union[
        "TaskiqResult[_ReturnType]",
        Coroutine[Any, Any, "TaskiqResult[_ReturnType]"],
    ]:
        """
        Wait for result to become ready and get it.

        This function constantly checks wheter result is ready
        and fetches it when it becomes available.

        :param check_interval: how ofen availability is checked.
        :param timeout: maximum amount of time it will wait
            before raising TaskiqResultTimeoutError.
        :param with_logs: whether you need to download logs.
        :return: TaskiqResult.
        """

    @abstractmethod
    def get_progress(
        self,
    ) -> Union[
        "Optional[TaskProgress[Any]]",
        Coroutine[Any, Any, "Optional[TaskProgress[Any]]"],
    ]:
        """
        Get task progress.

        :return: task's progress.
        """


class AsyncTaskiqTask(_Task[_ReturnType]):
    """AsyncTask for AsyncResultBackend."""

    def __init__(
        self,
        task_id: str,
        result_backend: "AsyncResultBackend[_ReturnType]",
    ) -> None:
        self.task_id = task_id
        self.result_backend = result_backend

    async def is_ready(self) -> bool:
        """
        Checks if task is completed.

        :raises ResultIsReadyError: if we can't get info about task readyness.

        :return: True if task is completed.
        """
        try:
            return await self.result_backend.is_result_ready(self.task_id)
        except Exception as exc:
            raise ResultIsReadyError() from exc

    async def get_result(self, with_logs: bool = False) -> "TaskiqResult[_ReturnType]":
        """
        Get result of a task from result backend.

        :param with_logs: whether you want to fetch logs from worker.

        :raises ResultGetError: if we can't get result from ResultBackend.

        :return: task's return value.
        """
        try:
            return await self.result_backend.get_result(
                self.task_id,
                with_logs=with_logs,
            )
        except Exception as exc:
            raise ResultGetError() from exc

    async def wait_result(
        self,
        check_interval: float = 0.2,
        timeout: float = -1.0,
        with_logs: bool = False,
    ) -> "TaskiqResult[_ReturnType]":
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
            if 0 < timeout < time() - start_time:
                raise TaskiqResultTimeoutError()
        return await self.get_result(with_logs=with_logs)

    async def get_progress(self) -> "Optional[TaskProgress[Any]]":
        """
        Get task progress.

        :return: task's progress.
        """
        return await self.result_backend.get_progress(self.task_id)
