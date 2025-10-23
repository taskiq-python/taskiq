import asyncio
from asyncio import Task
from logging import getLogger
from typing import Any

from taskiq.abc import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.api import run_receiver_task
from taskiq.message import TaskiqMessage
from taskiq.metrics import Stat
from taskiq.result import TaskiqResult

logger = getLogger(__name__)


class StatMiddleware(TaskiqMiddleware):
    """
    Middleware that gathers stat info.

    StatMiddleware runs on every worker it was added to.
    So to gather statistics from each worker we need additional PUB-SUB broker.

    This middleware starts internal worker process for stat_broker.

    If stat_broker was passed to __init__, and middleware starts by broker process,
    it also starts another worker for stat_broker to be able to reply on get_stats_task
    of each worker process.
    :param stat_broker: Any AsyncBroker (better be pub-sub type)
    """

    def __init__(self, stat_broker: AsyncBroker | None = None) -> None:
        super().__init__()
        self.stat_broker: AsyncBroker | None = stat_broker
        self.stat_worker: Task[Any] | None = None
        self.stat_metrics: Stat = Stat()

    async def startup(self) -> None:
        """
        Startup event trigger.

        If stat_broker was passed to __init__ and we start on the worker side,
        then we start stat_broker worker task to be able to run stat_broker tasks.
        """
        logger.info("StatMiddleware startup")
        if self.broker.is_worker_process and self.stat_broker:
            self.stat_broker.is_worker_process = True
            await self.stat_broker.startup()
            self.stat_worker = asyncio.create_task(run_receiver_task(self.stat_broker))

    async def shutdown(self) -> None:
        """Shutdown event trigger."""
        logger.info("StatMiddleware shutdown")
        if self.stat_worker:
            self.stat_worker.cancel()
            if self.stat_broker:
                await self.stat_broker.shutdown()

    def get_stats(self) -> Stat:
        """Returns dump of counters."""
        return self.stat_metrics

    def pre_execute(
        self,
        message: TaskiqMessage,
    ) -> TaskiqMessage:
        """
        Function to track received tasks.

        This function increments a counter of received tasks,
        when called.

        :param message: current message.
        :return: message
        """
        self.stat_metrics.received_tasks.label(message.task_name).inc()
        return message

    def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        """
        This function tracks number of errors and success executions.

        :param message: received message.
        :param result: result of the execution.
        """
        if result.is_err:
            self.stat_metrics.task_errors.label(message.task_name).inc()
        self.stat_metrics.execution_time.label(message.task_name).aggregate(
            result.execution_time,
        )
