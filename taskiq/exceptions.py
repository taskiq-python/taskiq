class TaskiqError(Exception):
    """Base exception for all errors."""


class TaskiqBrokerIsNotSetError(TaskiqError):
    """
    Someone tried to access non existing broker.

    This exception is thrown when you
    call `kiq()` method before calling `set_broker`
    function.
    """


class TaskiqResultTimeoutError(TaskiqError):
    """Waiting for task results has timed out."""
