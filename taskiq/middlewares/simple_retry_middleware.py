from logging import getLogger
from typing import Any, Iterable, Optional

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.exceptions import NoResultError
from taskiq.kicker import AsyncKicker
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

logger = getLogger("taskiq.retry_middleware")


class SimpleRetryMiddleware(TaskiqMiddleware):
    """Middleware to add retries."""

    def __init__(
        self,
        default_retry_count: int = 3,
        default_retry_label: bool = False,
        no_result_on_retry: bool = True,
        types_of_exceptions: Optional[Iterable[type[BaseException]]] = None,
    ) -> None:
        self.default_retry_count = default_retry_count
        self.default_retry_label = default_retry_label
        self.no_result_on_retry = no_result_on_retry
        self.types_of_exceptions = types_of_exceptions

    async def on_error(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
        exception: BaseException,
    ) -> None:
        """
        Retry on error.

        This middleware is used to retry
        tasks on errors.

        If error is found during the execution
        this function is invoked.

        :param message: Message that caused the error.
        :param result: execution result.
        :param exception: found exception.
        """
        if self.types_of_exceptions is not None and not isinstance(
            exception, tuple(self.types_of_exceptions)
        ):
            return

        # Valid exception
        if isinstance(exception, NoResultError):
            return

        retry_on_error = message.labels.get("retry_on_error")
        if isinstance(retry_on_error, str):
            retry_on_error = retry_on_error.lower() == "true"

        if retry_on_error is None:
            retry_on_error = self.default_retry_label
        # Check if retrying is enabled for the task.
        if not retry_on_error:
            return

        kicker: AsyncKicker[Any, Any] = AsyncKicker(
            task_name=message.task_name,
            broker=self.broker,
            labels=message.labels,
        ).with_task_id(message.task_id)

        # Getting number of previous retries.
        retries = int(message.labels.get("_retries", 0)) + 1
        kicker.with_labels(_retries=retries)
        max_retries = int(message.labels.get("max_retries", self.default_retry_count))

        if retries < max_retries:
            logger.info(
                "Task '%s' invocation failed. Retrying.",
                message.task_name,
            )
            await kicker.kiq(*message.args, **message.kwargs)

            if self.no_result_on_retry:
                result.error = NoResultError()

        else:
            logger.warning(
                "Task '%s' invocation failed. Maximum retries count is reached.",
                message.task_name,
            )
