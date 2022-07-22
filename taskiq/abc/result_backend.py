import asyncio
from abc import ABC, abstractmethod
from time import time
from typing import Any, Callable, Coroutine, Generic, TypeVar, overload
from uuid import uuid4

from taskiq.exceptions import TaskiqResultTimeoutError

_T = TypeVar("_T")  # noqa: WPS111
_ReturnType = TypeVar("_ReturnType")


def default_id_generator() -> str:
    """
    Generates unique task identifiers.

    This function returns unique task
    identifiers so we can easily find
    results of a task by this string.

    :return: unique string.
    """
    return f"{uuid4().hex}{time()}"


class AsyncResultBackend(ABC, Generic[_ReturnType]):
    """Async result backend."""

    def __init__(
        self,
        id_generator: Callable[[], str] = default_id_generator,
    ) -> None:
        self.id_generator = id_generator

    def generate_task(
        self,
    ) -> "AsyncTaskiqTask[_ReturnType]":
        """
        Generates new task.

        This function creates new AsyncTaskiqTask
        that returned to client after calling kiq
        method.

        :return: task object.
        """
        return AsyncTaskiqTask(task_id=self.id_generator(), result_backend=self)

    @abstractmethod
    async def set_result(self, task_id: str, result: Any) -> None:
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

    @overload
    async def get_result(  # noqa: D102
        self: "AsyncResultBackend[Coroutine[Any, Any, _T]]",
        task_id: str,
    ) -> _T:
        ...

    @overload
    async def get_result(  # noqa: D102
        self: "AsyncResultBackend[_ReturnType]",
        task_id: str,
    ) -> _ReturnType:
        ...

    @abstractmethod
    async def get_result(self, task_id: str) -> Any:
        """
        Gets result from the task.

        :param task_id: task's id.
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

    @overload
    async def get_result(  # noqa: D102
        self: "AsyncTaskiqTask[Coroutine[Any, Any, _T]]",
    ) -> _T:
        ...

    @overload
    async def get_result(  # noqa: D102
        self,
    ) -> _ReturnType:
        ...

    async def get_result(self) -> Any:
        """
        Get result of a task from result backend.

        :return: task's return value.
        """
        return await self.result_backend.get_result(self.task_id)

    @overload
    async def wait_result(  # noqa: D102
        self: "AsyncTaskiqTask[Coroutine[Any, Any, _T]]",
        check_interval: float = 0.1,
        timeout: float = 5.0,
    ) -> _T:
        ...

    @overload
    async def wait_result(  # noqa: D102
        self,
        check_interval: float = 0.1,
        timeout: float = 5.0,
    ) -> _ReturnType:
        ...

    async def wait_result(
        self,
        check_interval: float = 1.0,
        timeout: float = 5.0,
    ) -> Any:
        """
        Waits until result is ready.

        This method just checks whether the task is
        ready. And if it is it returns the result.

        It may throw TaskiqResultTimeoutError if
        task didn't became ready in provided
        period of time.

        :param check_interval: How often checks are performed.
        :param timeout: timeout for the result.
        :raises TaskiqResultTimeoutError: if task didn't
            become ready in provided period of time.
        :return: task's return value.
        """
        start_time = time()
        while not await self.is_ready():
            await asyncio.sleep(check_interval)
            if time() - start_time > timeout:
                raise TaskiqResultTimeoutError()
        return await self.get_result()
