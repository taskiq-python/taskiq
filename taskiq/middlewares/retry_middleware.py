from copy import deepcopy
from logging import getLogger
from typing import Any

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

logger = getLogger("taskiq.retry_middleware")


class SimpleRetryMiddleware(TaskiqMiddleware):
    """Middleware to add retries."""

    def __init__(
        self,
        default_retry_count: int = 3,
    ) -> None:
        self.default_retry_count = default_retry_count

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
        retry_on_error = message.labels.get("retry_on_error")
        # Check if retrying is enabled for the task.
        if retry_on_error is None or retry_on_error.lower() != "true":
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
            new_msg.labels["_parent"] = message.task_id
            new_msg.task_id = self.broker.id_generator()
            broker_message = self.broker.formatter.dumps(message=new_msg)
            await self.broker.kick(broker_message)
        else:
            logger.warning(
                "Task '%s' invocation failed. Maximum retries count is reached.",
                message.task_name,
            )
