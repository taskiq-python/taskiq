import os
from logging import getLogger
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Optional

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

logger = getLogger("taskiq.prometheus")


class PrometheusMiddleware(TaskiqMiddleware):
    """
    Middleware that adds prometheus metrics for workers.

    This middleware starts wsgi server with prometheus metrics.
    Also it updates metrics on events.

    :param server_port: meme
    """

    def __init__(
        self,
        metrics_path: Optional[Path] = None,
        server_port: int = 9000,
        server_addr: str = "0.0.0.0",  # noqa: S104
    ) -> None:
        super().__init__()

        self.found_errors = None
        self.received_tasks = None
        self.success_tasks = None
        self.saved_results = None
        self.execution_time = None

        if not AsyncBroker.is_worker_process:
            return

        metrics_path = metrics_path or Path(gettempdir()) / "taskiq_worker"

        if not metrics_path.exists():
            metrics_path.mkdir(parents=True)

        logger.debug(f"Setting up multiproc dir to {metrics_path}")

        os.environ["PROMETHEUS_MULTIPROC_DIR"] = str(metrics_path)
        os.environ["prometheus_multiproc_dir"] = str(metrics_path)

        logger.debug("Initializing metrics")

        try:
            from prometheus_client import (  # noqa: WPS433
                Counter,
                Histogram,
                start_http_server,
            )
        except ImportError as exc:
            raise ImportError(
                "Cannot initialize metrics. Please install 'taskiq[metrics]'.",
            ) from exc

        self.found_errors = Counter(
            "found_errors",
            "Number of found errors",
            ["task_name"],
        )
        self.received_tasks = Counter(
            "received_tasks",
            "Number of received tasks",
            ["task_name"],
        )
        self.success_tasks = Counter(
            "success_tasks",
            "Number of successfully executed tasks",
            ["task_name"],
        )
        self.saved_results = Counter(
            "saved_results",
            "Number of saved results in result backend",
            ["task_name"],
        )
        self.execution_time = Histogram(
            "execution_time",
            "Tome of function execution",
            ["task_name"],
        )
        try:
            start_http_server(port=server_port, addr=server_addr)
        except OSError as exc:
            logger.debug("Cannot start prometheus server: %s", exc)

    def pre_execute(
        self,
        message: "TaskiqMessage",
    ) -> "TaskiqMessage":
        """
        Function to track received tasks.

        This function increments a counter of received tasks,
        when called.

        :param message: current message.
        :return: message
        """
        if self.received_tasks is None:
            return message
        self.received_tasks.labels(message.task_name).inc()
        return message

    def post_execute(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> None:
        """
        This function tracks number of errors and success executions.

        :param message: received message.
        :param result: result of the execution.
        """
        if (  # noqa: WPS337
            self.success_tasks is None
            or self.execution_time is None
            or self.found_errors is None
        ):
            return
        if result.is_err:
            self.found_errors.labels(message.task_name).inc()
        else:
            self.success_tasks.labels(message.task_name).inc()
        self.execution_time.labels(message.task_name).observe(result.execution_time)

    def post_save(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> "None":
        """
        Method to run on save.

        :param message: received message.
        :param result: result of execution.
        """
        if self.saved_results is None:
            return
        self.saved_results.labels(message.task_name).inc()
