import asyncio
from logging import getLogger
from time import time
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from taskiq.compat import parse_obj_as
from taskiq.exceptions import (
    ResultGetError,
    ResultIsReadyError,
    TaskiqResultTimeoutError,
)

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.result_backend import AsyncResultBackend
    from taskiq.depends.progress_tracker import TaskProgress
    from taskiq.result import TaskiqResult

logger = getLogger("taskiq.task")

_ReturnType = TypeVar("_ReturnType")


class AsyncTaskiqTask(Generic[_ReturnType]):
    """AsyncTask for AsyncResultBackend."""

    def __init__(
        self,
        task_id: str,
        result_backend: "AsyncResultBackend[_ReturnType]",
        return_type: type[_ReturnType] | None = None,
    ) -> None:
        self.task_id = task_id
        self.result_backend = result_backend
        self.return_type = return_type

    async def is_ready(self) -> bool:
        """
        Checks if task is completed.

        :raises ResultIsReadyError: if we can't get info about task readiness.

        :return: True if task is completed.
        """
        try:
            return await self.result_backend.is_result_ready(self.task_id)
        except Exception as exc:
            raise ResultIsReadyError from exc

    async def get_result(self, with_logs: bool = False) -> "TaskiqResult[_ReturnType]":
        """
        Get result of a task from result backend.

        :param with_logs: whether you want to fetch logs from worker.

        :raises ResultGetError: if we can't get result from ResultBackend.

        :return: task's return value.
        """
        try:
            res = await self.result_backend.get_result(
                self.task_id,
                with_logs=with_logs,
            )
            if self.return_type is not None:
                try:
                    res.return_value = parse_obj_as(
                        self.return_type,
                        res.return_value,
                    )
                except ValueError:
                    logger.warning("Cannot parse return type into %s", self.return_type)
            return res
        except Exception as exc:
            raise ResultGetError from exc

    async def wait_result(
        self,
        check_interval: float = 0.2,
        timeout: float = -1.0,
        with_logs: bool = False,
        max_poll_failures: int = 3,
    ) -> "TaskiqResult[_ReturnType]":
        """
        Waits until result is ready.

        This method just checks whether the task is
        ready. And if it is it returns the result.

        It may throw TaskiqResultTimeoutError if
        task didn't become ready in provided
        period of time.

        Failing readiness checks are tolerated while they look
        transient. The counter is reset by every successful check, so
        the wait is only aborted after max_poll_failures checks have
        failed in a row.

        :param check_interval: How often checks are performed.
        :param timeout: timeout for the result.
        :param with_logs: whether you want to fetch logs from worker.
        :param max_poll_failures: how many readiness checks may fail
            in a row before giving up. Pass 0 to give up on the first
            failed check.
        :raises TaskiqResultTimeoutError: if task didn't
            become ready in provided period of time.
        :raises ResultIsReadyError: if readiness checks kept failing.
        :return: task's return value.
        """
        start_time = time()
        failures = 0
        while True:
            try:
                ready = await self.is_ready()
            except ResultIsReadyError:
                failures += 1
                if failures > max_poll_failures:
                    raise
                logger.warning(
                    "Cannot check readiness of task %s, "
                    "retrying (%d of %d attempts used).",
                    self.task_id,
                    failures,
                    max_poll_failures,
                    exc_info=True,
                )
            else:
                if ready:
                    break
                failures = 0
            if 0 < timeout < time() - start_time:
                raise TaskiqResultTimeoutError(timeout=timeout)
            await asyncio.sleep(check_interval)
        return await self.get_result(with_logs=with_logs)

    async def get_progress(self) -> "TaskProgress[Any] | None":
        """
        Get task progress.

        :return: task's progress.
        """
        return await self.result_backend.get_progress(self.task_id)
