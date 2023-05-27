from copy import deepcopy
from logging import getLogger
from typing import Any

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.exceptions import NoResultError
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
    ) -> None:
        self.default_retry_count = default_retry_count
        self.default_retry_label = default_retry_label
        self.no_result_on_retry = no_result_on_retry

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
        # Valid exception
        if isinstance(exception, NoResultError):
            return

        retry_on_error = message.labels.get("retry_on_error")
        if retry_on_error is None:
            retry_on_error = "true" if self.default_retry_label else "false"
        # Check if retrying is enabled for the task.
        if retry_on_error.lower() != "true":
            return
        new_msg = deepcopy(message)

        # Getting number of previous retries.
        retries = int(new_msg.labels.get("_retries", 0)) + 1
        new_msg.labels["_retries"] = str(retries)
        max_retries = int(new_msg.labels.get("max_retries", self.default_retry_count))

        if retries < max_retries:
            logger.info(
                "Task '%s' invocation failed. Retrying.",
                message.task_name,
            )
            broker_message = self.broker.formatter.dumps(message=new_msg)
            await self.broker.kick(broker_message)

            if self.no_result_on_retry:
                result.error = NoResultError()

        else:
            logger.warning(
                "Task '%s' invocation failed. Maximum retries count is reached.",
                message.task_name,
            )
