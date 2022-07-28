class TaskiqError(Exception):
    """Base exception for all errors."""


class TaskiqResultTimeoutError(TaskiqError):
    """Waiting for task results has timed out."""
