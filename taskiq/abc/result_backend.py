from abc import ABC, abstractmethod
from typing import Generic

from taskiq.result import TaskiqResult
from taskiq.task import AsyncTaskiqTask
from taskiq.types_helpers import ReturnType_


class AsyncResultBackend(ABC, Generic[ReturnType_]):
    """Async result backend."""

    async def startup(self) -> None:
        """Do something when starting broker."""

    async def shutdown(self) -> None:
        """Do something on shutdown."""

    def generate_task(self, task_id: str) -> "AsyncTaskiqTask[ReturnType_]":
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
    async def set_result(self, task_id: str, result: TaskiqResult[ReturnType_]) -> None:
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
    ) -> TaskiqResult[ReturnType_]:
        """
        Gets result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :return: task's return value.
        """
