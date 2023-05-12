from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar

from taskiq.result import TaskiqResult

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.depends.progress_tracker import TaskProgress


_ReturnType = TypeVar("_ReturnType")


class AsyncResultBackend(ABC, Generic[_ReturnType]):
    """Async result backend."""

    async def startup(self) -> None:
        """Do something when starting broker."""

    async def shutdown(self) -> None:
        """Do something on shutdown."""

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

    async def set_progress(
        self,
        task_id: str,
        progress: "TaskProgress[Any]",
    ) -> None:
        """
        Saves progress.

        :param task_id: task's id.
        :param progress: progress of execution.
        """

    async def get_progress(
        self,
        task_id: str,
    ) -> "Optional[TaskProgress[Any]]":
        """
        Gets progress.

        :param task_id: task's id.
        """
