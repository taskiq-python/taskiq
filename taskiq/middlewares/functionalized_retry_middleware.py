from copy import deepcopy
from logging import getLogger
from typing import Any, Callable, Dict

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.kicker import AsyncKicker
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

logger = getLogger("taskiq.functionalized_retry_middleware")


def default_exponential_delay(retries_count: int, **kwargs: Any) -> float:
    """
    Returns new delay as a float number.

    :param retries_count: number of retries.
    :param kwargs: named arguments.

    :returns: delay as a float number.
    """
    return float(2**retries_count)


class FunctionalizedRetryMiddleware(TaskiqMiddleware):
    """Middleware to add retries with special rules."""

    def __init__(
        self,
        strategy_function: Callable[..., float] = default_exponential_delay,
        strategy_name: str = "exponential",
        default_retry_count: int = 3,
    ) -> None:
        """
        Initialized middleware.

        You can pass function to define time for delay.
        Also you must name `strategy_name`, if you want to see
        actual information about task execution.

        :param strategy_function: Callable object that returns int.
        :param strategy_name: name of the strategy.
        :param default_retry_count: maximum number of retries.
        """
        self.default_retry_count = default_retry_count
        self.strategy_function = strategy_function
        self.strategy_name = strategy_name

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
            delay = self.strategy_function(
                retries_count=retries,
                taskiq_msg=new_msg,
            )
            new_msg.labels["_parent"] = message.task_id

            await AsyncKicker(
                new_msg.task_name,
                self.broker,
                new_msg.labels,
            ).with_labels(
                delay=delay,
                _parent=message.task_id,
                _retries=retries,
                _delay_strategy=self.strategy_name,
            ).kiq(
                *message.args, **message.kwargs,
            )
        else:
            logger.warning(
                "Task '%s' invocation failed. Maximum retries count is reached.",
                message.task_name,
            )
