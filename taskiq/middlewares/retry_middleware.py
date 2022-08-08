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
        exception: Exception,
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
        if retry_on_error != "True":
            return

        logger.info(f"Task '{message.task_name}' invocation failed. Retrying.")

        # Getting number of previous retries.
        retries = int(message.labels.get("_retries", 0)) + 1
        message.labels["_retries"] = str(retries)
        max_retries = int(message.labels.get("max_retries", self.default_retry_count))
        if retries < max_retries:
            message.task_id = self.broker.id_generator()
            broker_message = self.broker.formatter.dumps(message=message)
            await self.broker.kick(broker_message)
