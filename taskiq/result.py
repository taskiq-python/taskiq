from typing import Generic, Optional, TypeVar

from pydantic.generics import GenericModel

_ReturnType = TypeVar("_ReturnType")


class TaskiqResult(GenericModel, Generic[_ReturnType]):
    """Result of a remote task invocation."""

    is_err: bool
    # Log is a deprecated field. It would be removed in future
    # releases of not, if we find a way to capture logs in async
    # environment.
    log: Optional[str] = None
    return_value: _ReturnType
    execution_time: float
