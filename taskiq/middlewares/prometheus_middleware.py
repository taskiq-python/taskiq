import datetime
import os
from logging import getLogger
from pathlib import Path
from tempfile import gettempdir
from typing import Any
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult
from taskiq.receiver.observer import ReceiverObserver

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
        metrics_path: Path | None = None,
        server_port: int = 9000,
        server_addr: str = "0.0.0.0",  # noqa: S104
    ) -> None:
        super().__init__()

        metrics_path = metrics_path or Path(gettempdir()) / "taskiq_worker"

        if not metrics_path.exists():
            metrics_path.mkdir(parents=True)

        logger.debug(f"Setting up multiproc dir to {metrics_path}")

        os.environ["PROMETHEUS_MULTIPROC_DIR"] = str(metrics_path)

        logger.debug("Initializing metrics")

        try:
            from prometheus_client import Counter, Histogram, Gauge  # noqa: PLC0415
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

        self.queue_wait_seconds = Histogram(
            "queue_wait_seconds",
            "time task spent in message queue",
            ["task_name"],
        )
        self.task_errors_by_type = Counter(
            "task_errors_by_type",
            "Number of errors raised in tasks by their type",
            ["task_name", "error_type"],
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

    def pre_send(
        self,
        message: "TaskiqMessage",
    ) -> "TaskiqMessage":
        """
        Function to track the time a task spend in queue.

        This function tracks the time a task spends in a queue until it is executed.

        :param message: current message.
        :return: message
        """
        if not message.labels.get("_taskiq_enqueue_timestamp"):
            message.labels["_taskiq_enqueue_timestamp"] = datetime.datetime.now(
                datetime.UTC,
            ).isoformat()  # Might conside using timezones too
        return message

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
        if message.labels.get(
            "_taskiq_enqueue_timestamp",
        ):  # Handle case where the sender doesn't use the prometheus middleware
            time_delta = datetime.datetime.now(
                datetime.UTC,
            ) - datetime.datetime.fromisoformat(
                message.labels["_taskiq_enqueue_timestamp"],
            )
            time_delta = max(0, time_delta.total_seconds())
            self.queue_wait_seconds.labels(message.task_name).observe(
                time_delta,
            )

        self.received_tasks.labels(message.task_name).inc()
        return message

    def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],  # pylint: disable=unused-argument
        exception: BaseException,
    ) -> None:
        """
        This function tracks the number of errors raised by tasks.

        :param message: the received task message
        :param result: the result of task
        :param exception: exception raised
        """
        self.task_errors_by_type.labels(
            message.task_name,
            type(exception).__name__,
        ).inc()

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

    def set_broker(self, broker: "AsyncBroker") -> None:  # noqa: F821 pyright: ignore[reportUnknownVariableType]
        super().set_broker(broker)
        broker._receiver_observer = PrometheusReceiverObserver()

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


class PrometheusReceiverObserver(ReceiverObserver):
    """Receiver observer implementation for prometheus."""

    def __init__(self) -> None:
        try:
            from prometheus_client import Counter, Gauge  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "Cannot initialize metrics. Please install 'taskiq[metrics]'.",
            ) from exc

        self.prefetch_queue_size = Gauge(
            "prefetch_queue_size",
            "The number of task in the prefetch queue.",
            multiprocess_mode="livesum",
        )
        self.semaphore_available = Gauge(
            "semaphore_available",
            "Number of semaphore slots available in broker",
            multiprocess_mode="livesum",
        )
        self.active_tasks_count = Gauge(
            "worker_active_tasks_count",
            "Number of active tasks in worker",
            multiprocess_mode="livesum",
        )
        self.task_not_found_total = Counter(
            "task_not_found_total",
            "Number of times the worker got a task not registered",
            ["task_name"],
        )
        self.deserialize_error = Counter(
            "deserialize_error_count",
            "Number of times broker faced a desrialization error",
        )

    def on_prefetch_queue_size(self, size: int) -> None:
        self.prefetch_queue_size.set(size)

    def on_semaphore_status(self, available: int) -> None:
        self.semaphore_available.set(available)

    def on_active_tasks_count(self, count: int) -> None:
        self.active_tasks_count.set(count)

    def on_task_not_found(self, task_name: str) -> None:
        self.task_not_found_total.labels(task_name).inc()

    def on_deserialize_error(self, raw: bytes, error: Exception) -> None:
        self.deserialize_error.inc()
