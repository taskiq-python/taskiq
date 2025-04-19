from __future__ import annotations

import datetime
import random
from logging import getLogger
from types import NoneType
from typing import Any

from taskiq import ScheduleSource
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.exceptions import NoResultError
from taskiq.kicker import AsyncKicker
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

__all__ = ("SmartRetryMiddleware",)

_logger = getLogger("taskiq.smart_retry_middleware")


class SmartRetryMiddleware(TaskiqMiddleware):
    """Middleware to retry tasks delays.

    This middleware retries failed tasks with support for:
    - max retries
    - delay
    - jitter
    - exponential backoff
    """

    def __init__(
        self,
        default_retry_count: int = 3,
        default_retry_label: bool = False,
        no_result_on_retry: bool = True,
        default_delay: float = 5,
        use_jitter: bool = False,
        use_delay_exponent: bool = False,
        max_delay_exponent: float = 60,
        schedule_source: ScheduleSource | None = None,
    ) -> None:
        """
        Initialize retry middleware.

        :param default_retry_count: Default max retries if not specified.
        :param default_retry_label: Whether to retry tasks by default.
        :param no_result_on_retry: Replace result with NoResultError on retry.
        :param default_delay: Delay in seconds before retrying.
        :param use_jitter: Add random jitter to retry delay.
        :param use_delay_exponent: Apply exponential backoff to delay.
        :param max_delay_exponent: Maximum allowed delay when using backoff.
        :param schedule_source: Schedule source to use for scheduling.
            If None, the default broker will be used.
        """
        super().__init__()
        self.default_retry_count = default_retry_count
        self.default_retry_label = default_retry_label
        self.no_result_on_retry = no_result_on_retry
        self.default_delay = default_delay
        self.use_jitter = use_jitter
        self.use_delay_exponent = use_delay_exponent
        self.max_delay_exponent = max_delay_exponent
        self.schedule_source = schedule_source

        if not isinstance(schedule_source, (ScheduleSource, NoneType)):
            raise TypeError(
                "schedule_source must be an instance of ScheduleSource or None",
            )

    def is_retry_on_error(self, message: TaskiqMessage) -> bool:
        """
        Check if retry is enabled for this task.

        Looks for `retry_on_error` label, falls back to default.

        :param message: Original task message.
        :return: True if should retry on error.
        """
        retry_on_error = message.labels.get("retry_on_error")
        if isinstance(retry_on_error, str):
            retry_on_error = retry_on_error.lower() == "true"
        if retry_on_error is None:
            retry_on_error = self.default_retry_label
        return retry_on_error

    def make_delay(self, message: TaskiqMessage, retries: int) -> float:
        """
        Calculate retry delay.

        Includes jitter and exponential backoff if enabled.

        :param message: Task message.
        :param retries: Current retry count.
        :return: Delay in seconds.
        """
        delay = float(message.labels.get("delay", self.default_delay))
        if self.use_delay_exponent:
            delay = min(delay * retries, self.max_delay_exponent)

        if self.use_jitter:
            delay += random.random()  # noqa: S311

        return delay

    async def on_send(
        self,
        kicker: AsyncKicker[Any, Any],
        message: TaskiqMessage,
        delay: float,
    ) -> None:
        """Execute the task with a delay."""
        if self.schedule_source is None:
            await kicker.with_labels(delay=delay).kiq(*message.args, **message.kwargs)
        else:
            target_time = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
                seconds=delay,
            )
            await kicker.schedule_by_time(
                self.schedule_source,
                target_time,
                *message.args,
                **message.kwargs,
            )

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
        exception: BaseException,
    ) -> None:
        """
        Retry on error.

        If an error is raised during task execution,
        this middleware schedules the task to be retried
        after a calculated delay.

        :param message: Message that caused the error.
        :param result: Execution result.
        :param exception: Caught exception.
        """
        if isinstance(exception, NoResultError):
            return

        retry_on_error = self.is_retry_on_error(message)

        if not retry_on_error:
            return

        retries = int(message.labels.get("_retries", 0)) + 1
        max_retries = int(message.labels.get("max_retries", self.default_retry_count))

        if retries < max_retries:
            delay = self.make_delay(message, retries)

            _logger.info(
                "Task %s failed. Retrying %d/%d in %.2f seconds.",
                message.task_name,
                retries,
                max_retries,
                delay,
            )

            kicker: AsyncKicker[Any, Any] = (
                AsyncKicker(
                    task_name=message.task_name,
                    broker=self.broker,
                    labels=message.labels,
                )
                .with_task_id(message.task_id)
                .with_labels(_retries=retries)
            )

            await self.on_send(kicker, message, delay)

            if self.no_result_on_retry:
                result.error = NoResultError()

        else:
            _logger.warning(
                "Task '%s' invocation failed. Maximum retries count is reached.",
                message.task_name,
            )
