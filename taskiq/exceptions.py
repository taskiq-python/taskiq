from typing import Optional

from izulu import root


class TaskiqError(root.Error):
    """Base exception for all errors."""

    __template__ = "Exception occurred: {description}"
    description: Optional[str] = None


class TaskiqResultTimeoutError(TaskiqError):
    """Waiting for task results has timed out."""

    __template__ = "Waiting for task results has timed out, timeout={timeout}"
    timeout: Optional[float] = None


class BrokerError(TaskiqError):
    """Base class for all broker errors."""

    __template__ = "Base exception for all broker errors"


class ListenError(TaskiqError):
    """Error if the broker is unable to listen to the queue."""


class SharedBrokerListenError(ListenError):
    """Error when someone tries to listen to the queue with shared broker."""

    __template__ = "Shared broker cannot listen"


class SendTaskError(BrokerError):
    """Error if the broker was unable to send the task to the queue."""

    __template__ = "Cannot send task to the queue"


class SharedBrokerSendTaskError(SendTaskError):
    """Error when someone tries to send task with shared broker."""

    __template__ = (
        "You cannot use kiq directly on shared task "
        "without setting the default_broker."
    )


class UnknownTaskError(SendTaskError):
    """Error if task is unknown."""

    __template__ = "Cannot send unknown task to the queue, task name - {task_name}"
    task_name: str


class ResultBackendError(TaskiqError):
    """Base class for all ResultBackend errors."""

    __template__ = "Base exception for all result backend errors"


class ResultGetError(ResultBackendError):
    """Error if ResultBackend was unable to get result."""

    __template__ = "Cannot get result for the task"


class ResultSetError(ResultBackendError):
    """Error if ResultBackend was unable to set result."""

    __template__ = "Cannot set result for the task"


class ResultIsReadyError(ResultBackendError):
    """Error if ResultBackend was unable to find out if the task is ready."""

    __template__ = "Cannot find out if the task is ready"


class SecurityError(TaskiqError):
    """Security related exception."""

    __template__ = "Security exception occurred: {description}"
    description: str


class NoResultError(TaskiqError):
    """Error if user does not want to set result."""


class TaskRejectedError(TaskiqError):
    """Task was rejected."""

    __template__ = "Task was rejected"


class ScheduledTaskCancelledError(TaskiqError):
    """Scheduled task was cancelled and not sent to the queue."""

    __template__ = "Cannot send scheduled task to the queue."
