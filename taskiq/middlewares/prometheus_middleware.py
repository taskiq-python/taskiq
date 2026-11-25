import os
from logging import getLogger
from pathlib import Path
from tempfile import gettempdir
from typing import Any

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

logger = getLogger("taskiq.prometheus")


class PrometheusMiddleware(TaskiqMiddleware):
    """
    Middleware that adds prometheus metrics for workers.

    This middleware starts wsgi server with prometheus metrics.
    Also it updates metrics on events.

    The middleware is import-safe: creating multiple instances
    in the same process (e.g. when the broker module is imported
    more than once during task discovery) reuses already registered
    collectors instead of raising
    ``ValueError: Duplicated timeseries in CollectorRegistry``.

    :param server_port: The port to listen on.
    :param server_addr: The address to listen on.
    :param metrics_path: The path to store metrics for multiproc env.
    """

    def __init__(
        self,
        metrics_path: Path | None = None,
        server_port: int = 9000,
        server_addr: str = "0.0.0.0",  # noqa: S104
    ) -> None:
        super().__init__()

        metrics_path = metrics_path or Path(gettempdir()) / "taskiq_worker"

        if not metrics_path.exists():
            metrics_path.mkdir(parents=True)

        logger.debug("Setting up multiproc dir to %s", metrics_path)

        os.environ["PROMETHEUS_MULTIPROC_DIR"] = str(metrics_path)

        logger.debug("Initializing metrics")

        try:
            from prometheus_client import (  # noqa: PLC0415
                REGISTRY,
                Counter,
                Histogram,
            )
        except ImportError as exc:
            raise ImportError(
                "Cannot initialize metrics. Please install 'taskiq[metrics]'.",
            ) from exc

        def _get_or_create_counter(
            name: str,
            documentation: str,
            labelnames: list[str],
        ) -> Counter:
            """Return existing counter or create a new one."""
            try:
                return Counter(name, documentation, labelnames)
            except ValueError:
                existing = REGISTRY._names_to_collectors.get(name)  # noqa: SLF001
                if existing is None or not isinstance(existing, Counter):
                    raise
                return existing

        def _get_or_create_histogram(
            name: str,
            documentation: str,
            labelnames: list[str],
        ) -> Histogram:
            """Return existing histogram or create a new one."""
            try:
                return Histogram(name, documentation, labelnames)
            except ValueError:
                existing = REGISTRY._names_to_collectors.get(name)  # noqa: SLF001
                if existing is None or not isinstance(existing, Histogram):
                    raise
                return existing

        self.found_errors = _get_or_create_counter(
            "found_errors",
            "Number of found errors",
            ["task_name"],
        )
        self.received_tasks = _get_or_create_counter(
            "received_tasks",
            "Number of received tasks",
            ["task_name"],
        )
        self.success_tasks = _get_or_create_counter(
            "success_tasks",
            "Number of successfully executed tasks",
            ["task_name"],
        )
        self.saved_results = _get_or_create_counter(
            "saved_results",
            "Number of saved results in result backend",
            ["task_name"],
        )
        self.execution_time = _get_or_create_histogram(
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
        from prometheus_client import (  # noqa: PLC0415
            CollectorRegistry,
            start_http_server,
        )
        from prometheus_client.multiprocess import (  # noqa: PLC0415
            MultiProcessCollector,
        )

        if self.broker.is_worker_process:
            try:
                registry = CollectorRegistry()
                MultiProcessCollector(registry)
                start_http_server(
                    port=self.server_port,
                    addr=self.server_addr,
                    registry=registry,
                )
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
