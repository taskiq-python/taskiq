class TaskiqError(Exception):
    """Base exception for all errors."""


class TaskiqResultTimeoutError(TaskiqError):
    """Waiting for task results has timed out."""


class BrokerError(TaskiqError):
    """Base class for all broker errors."""


class SendTaskError(BrokerError):
    """Error if the broker was unable to send the task to the queue."""


class ResultBackendError(TaskiqError):
    """Base class for all ResultBackend errors."""


class ResultGetError(ResultBackendError):
    """Error if ResultBackend was unable to get result."""


class ResultSetError(ResultBackendError):
    """Error if ResultBackend was unable to set result."""


class ResultIsReadyError(ResultBackendError):
    """Error if ResultBackend was unable to find out if the task is ready."""


class SecurityError(TaskiqError):
    """Security related exception."""


class NoResultError(TaskiqError):
    """Error if user does not want to set result."""


class RejectError(TaskiqError):
    """Error is thrown if message should be rejected."""
