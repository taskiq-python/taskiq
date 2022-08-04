from typing import Generic, Optional, TypeVar

from pydantic.generics import GenericModel

_ReturnType = TypeVar("_ReturnType")


class TaskiqResult(GenericModel, Generic[_ReturnType]):
    """Result of a remote task invocation."""

    is_err: bool
    log: Optional[str]
    return_value: _ReturnType
    execution_time: float
