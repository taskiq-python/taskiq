import asyncio
from logging import getLogger
from time import time
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from taskiq.compat import parse_obj_as
from taskiq.exceptions import (
    ResultBackendError,
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

# Upper bound for the delay between polls while the result backend is failing.
# It never goes below the user-provided check interval.
_MAX_BACKEND_ERROR_DELAY = 5.0


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
        retry_on_backend_error: bool = False,
    ) -> "TaskiqResult[_ReturnType]":
        """
        Waits until result is ready.

        This method just checks whether the task is
        ready. And if it is it returns the result.

        It may throw TaskiqResultTimeoutError if
        task didn't become ready in provided
        period of time.

        By default, any error of the result backend ends the wait
        immediately. A failed poll says nothing about the task itself,
        so for network-backed result backends it may be preferable to
        keep waiting. Pass `retry_on_backend_error=True` to treat
        result backend errors, both when checking readiness and when
        fetching the result, as "not ready yet": the poll is retried
        with an increasing delay until the timeout expires.

        :param check_interval: How often checks are performed.
        :param timeout: timeout for the result.
        :param with_logs: whether you want to fetch logs from worker.
        :param retry_on_backend_error: retry polling when the result
            backend raises an error instead of failing right away.
            Without a timeout, the polling is retried indefinitely.
        :raises TaskiqResultTimeoutError: if task didn't
            become ready in provided period of time. If the last poll
            failed with a result backend error, it is set as the cause.
        :raises ResultIsReadyError: if the readiness check failed and
            `retry_on_backend_error` is disabled.
        :raises ResultGetError: if fetching the result failed and
            `retry_on_backend_error` is disabled.
        :return: task's return value.
        """
        start_time = time()
        last_error: ResultBackendError | None = None
        delay = check_interval
        while True:
            try:
                if await self.is_ready():
                    return await self.get_result(with_logs=with_logs)
                last_error = None
                delay = check_interval
            except ResultBackendError as exc:
                if not retry_on_backend_error:
                    raise
                logger.warning(
                    "Result backend failed while waiting for the result of "
                    "task %s, retrying: %r",
                    self.task_id,
                    exc.__cause__ or exc,
                )
                last_error = exc
                delay = min(delay * 2, max(check_interval, _MAX_BACKEND_ERROR_DELAY))
            elapsed = time() - start_time
            if 0 < timeout < elapsed:
                raise TaskiqResultTimeoutError(timeout=timeout) from last_error
            sleep_for = delay
            if last_error is not None and timeout > 0:
                # Don't sleep past the deadline because of the backoff.
                sleep_for = min(delay, max(timeout - elapsed, 0))
            await asyncio.sleep(sleep_for)

    async def get_progress(self) -> "TaskProgress[Any] | None":
        """
        Get task progress.

        :return: task's progress.
        """
        return await self.result_backend.get_progress(self.task_id)
