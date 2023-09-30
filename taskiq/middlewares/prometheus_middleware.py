import os
from logging import getLogger
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Optional

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

logger = getLogger("taskiq.prometheus")


class PrometheusMiddleware(TaskiqMiddleware):
    """
    Middleware that adds Prometheus metrics for workers.

    This middleware starts a WSGI server with Prometheus metrics.
    It also updates metrics on events.

    :param server_port: Port for the Prometheus server.
    """

    def __init__(
        self,
        metrics_path: Optional[Path] = None,
        server_port: int = 9000,
        server_addr: str = "0.0.0.0",  # noqa: S104 (No longer needed)
    ) -> None:
        super().__init__()

        metrics_path = metrics_path or Path(gettempdir()) / "taskiq_worker"

        if not metrics_path.exists():
            metrics_path.mkdir(parents=True)

        logger.debug(f"Setting up multiproc dir to {metrics_path}")

        os.environ["PROMETHEUS_MULTIPROC_DIR"] = str(metrics_path)
        os.environ["prometheus_multiproc_dir"] = str(metrics_path)

        logger.debug("Initializing metrics")

        try:
            from prometheus_client import Counter, Histogram  # noqa: WPS433 (No longer needed)
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
            "Number of saved results in the result backend",
            ["task_name"],
        )
        self.execution_time = Histogram(
            "execution_time",
            "Time of function execution",
            ["task_name"],
        )
        self.server_port = server_port
        self.server_addr = server_addr

    def startup(self) -> None:
        """
        Prometheus startup.

        This function starts the Prometheus server.
        It starts it only in case it's a worker process.
        """
        from prometheus_client import start_http_server  # noqa: WPS433 (No longer needed)

        if self.broker.is_worker_process:
            try:
                start_http_server(port=self.server_port, addr=self.server_addr)
            except OSError as exc:
                logger.debug("Cannot start Prometheus server: %s", exc)

    def pre_execute(
        self,
        message: TaskiqMessage,
    ) -> TaskiqMessage:
        """
        Function to track received tasks.

        This function increments a counter of received tasks when called.

        :param message: Current message.
        :return: Message
        """
        self.received_tasks.labels(message.task_name).inc()
        return message

    def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        """
        This function tracks the number of errors and successful executions.

        :param message: Received message.
        :param result: Result of the execution.
        """
        if result.is_err:
            self.found_errors.labels(message.task_name).inc()
        else:
            self.success_tasks.labels(message.task_name).inc()
        self.execution_time.labels(message.task_name).observe(result.execution_time)

    def post_save(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        """
        Method to run on save.

        :param message: Received message.
        :param result: Result of execution.
        """
        self.saved_results.labels(message.task_name).inc()
