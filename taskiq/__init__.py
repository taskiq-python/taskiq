"""Distributed task manager."""
from taskiq_dependencies import Depends as TaskiqDepends

from taskiq.abc.broker import AsyncBroker, AsyncTaskiqDecoratedTask
from taskiq.abc.formatter import TaskiqFormatter
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.acks import AckableMessage
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.brokers.shared_broker import async_shared_broker
from taskiq.brokers.zmq_broker import ZeroMQBroker
from taskiq.context import Context
from taskiq.events import TaskiqEvents
from taskiq.exceptions import (
    NoResultError,
    RejectError,
    ResultGetError,
    ResultIsReadyError,
    SecurityError,
    SendTaskError,
    TaskiqError,
    TaskiqResultTimeoutError,
)
from taskiq.funcs import gather
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.middlewares.prometheus_middleware import PrometheusMiddleware
from taskiq.middlewares.retry_middleware import SimpleRetryMiddleware
from taskiq.result import TaskiqResult
from taskiq.scheduler import ScheduledTask, TaskiqScheduler
from taskiq.state import TaskiqState
from taskiq.task import AsyncTaskiqTask

try:
    # Python 3.8+
    from importlib.metadata import version  # noqa: WPS433
except ImportError:
    # Python 3.7
    from importlib_metadata import version  # noqa: WPS433

__version__ = version("taskiq")
__all__ = [
    "__version__",
    "gather",
    "Context",
    "AsyncBroker",
    "TaskiqError",
    "RejectError",
    "TaskiqState",
    "TaskiqResult",
    "ZeroMQBroker",
    "TaskiqEvents",
    "SecurityError",
    "TaskiqMessage",
    "BrokerMessage",
    "ResultGetError",
    "ScheduledTask",
    "TaskiqDepends",
    "NoResultError",
    "SendTaskError",
    "AckableMessage",
    "InMemoryBroker",
    "ScheduleSource",
    "TaskiqScheduler",
    "TaskiqFormatter",
    "AsyncTaskiqTask",
    "TaskiqMiddleware",
    "ResultIsReadyError",
    "AsyncResultBackend",
    "async_shared_broker",
    "PrometheusMiddleware",
    "SimpleRetryMiddleware",
    "AsyncTaskiqDecoratedTask",
    "TaskiqResultTimeoutError",
]
