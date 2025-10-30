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
    Middleware that adds prometheus metrics for workers.

    This middleware starts wsgi server with prometheus metrics.
    Also it updates metrics on events.

    :param server_port: The port to listen on.
    :param server_addr: The address to listen on.
    :paam metrics_path: The path to store metrics for multiproc env.
    """

    def __init__(
        self,
        metrics_path: Optional[Path] = None,
        server_port: int = 9000,
        server_addr: str = "0.0.0.0",  # noqa: S104
    ) -> None:
        super().__init__()

        metrics_path = metrics_path or Path(gettempdir()) / "taskiq_worker"

        if not metrics_path.exists():
            metrics_path.mkdir(parents=True)

        logger.debug(f"Setting up multiproc dir to {metrics_path}")

        os.environ["PROMETHEUS_MULTIPROC_DIR"] = str(metrics_path)
        os.environ["PROMETHEUS_MULTIPROC_DIR"] = str(metrics_path)

        logger.debug("Initializing metrics")

        try:
            from prometheus_client import Counter, Histogram  # noqa: PLC0415
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
            "Time of function execution",
            ["task_name"],
        )
        self.server_port = server_port
        self.server_addr = server_addr

    def startup(self) -> None:
        """
        Prometheus startup.

        This function starts prometheus server.
        It starts it only in case if it's a worker process.
        """
        from prometheus_client import start_http_server  # noqa: PLC0415

        if self.broker.is_worker_process:
            try:
                start_http_server(port=self.server_port, addr=self.server_addr)
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
        self.saved_results.labels(message.task_name).inc()
