class TaskiqError(Exception):
    """Base class for all Taskiq errors."""


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
