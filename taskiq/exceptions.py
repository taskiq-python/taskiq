from izulu import root


class TaskiqError(root.Error):
    """Base exception for all errors."""

    __template__ = "Exception occurred: {description}"
    description: str


class TaskiqResultTimeoutError(TaskiqError):
    """Waiting for task results has timed out."""

    __template__ = "Waiting for task results has timed out, timeout={timeout}"
    timeout: float


class BrokerError(TaskiqError):
    """Base class for all broker errors."""

    __template__ = "Base exception for all broker errors"


class SendTaskError(BrokerError):
    """Error if the broker was unable to send the task to the queue."""

    __template__ = "Cannot send task to the queue"


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

    __template__ = "Base exception for all security errors"


class NoResultError(TaskiqError):
    """Error if user does not want to set result."""


class TaskRejectedError(TaskiqError):
    """Task was rejected."""

    __template__ = "Task was rejected"


class ScheduledTaskCancelledError(TaskiqError):
    """Scheduled task was cancelled and not sent to the queue."""

    __template__ = "Cannot send scheduled task to the queue."
