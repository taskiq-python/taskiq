"""Distributed task manager."""

from importlib.metadata import version

from taskiq_dependencies import Depends as TaskiqDepends

from taskiq.abc.broker import AsyncBroker, AsyncTaskiqDecoratedTask
from taskiq.abc.formatter import TaskiqFormatter
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.acks import AckableMessage, AcknowledgeType
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.brokers.shared_broker import async_shared_broker
from taskiq.brokers.zmq_broker import ZeroMQBroker
from taskiq.context import Context
from taskiq.events import TaskiqEvents
from taskiq.exceptions import (
    NoResultError,
    ResultGetError,
    ResultIsReadyError,
    SecurityError,
    SendTaskError,
    TaskiqError,
    TaskiqResultTimeoutError,
    UnsupportedFlowError,
)
from taskiq.flow import Flow, FlowIdentity, FlowProtocol, get_flow_identity
from taskiq.funcs import gather
from taskiq.kicker import PreparedKiq
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.middlewares import (
    PrometheusMiddleware,
    SimpleRetryMiddleware,
    SmartRetryMiddleware,
)
from taskiq.result import TaskiqResult
from taskiq.router import TaskiqRoute, TaskiqRouter, TaskiqSubscription
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler
from taskiq.state import TaskiqState
from taskiq.task import AsyncTaskiqTask
from taskiq.task_builder import TaskDefinition, task_builder

__version__ = version("taskiq")

__all__ = [
    "AckableMessage",
    "AcknowledgeType",
    "AsyncBroker",
    "AsyncResultBackend",
    "AsyncTaskiqDecoratedTask",
    "AsyncTaskiqTask",
    "BrokerMessage",
    "Context",
    "Flow",
    "FlowIdentity",
    "FlowProtocol",
    "InMemoryBroker",
    "NoResultError",
    "PreparedKiq",
    "PrometheusMiddleware",
    "ResultGetError",
    "ResultIsReadyError",
    "ScheduleSource",
    "ScheduledTask",
    "SecurityError",
    "SendTaskError",
    "SimpleRetryMiddleware",
    "SmartRetryMiddleware",
    "TaskDefinition",
    "TaskiqDepends",
    "TaskiqError",
    "TaskiqEvents",
    "TaskiqFormatter",
    "TaskiqMessage",
    "TaskiqMiddleware",
    "TaskiqResult",
    "TaskiqResultTimeoutError",
    "TaskiqRoute",
    "TaskiqRouter",
    "TaskiqScheduler",
    "TaskiqState",
    "TaskiqSubscription",
    "UnsupportedFlowError",
    "ZeroMQBroker",
    "__version__",
    "async_shared_broker",
    "gather",
    "get_flow_identity",
    "task_builder",
]
