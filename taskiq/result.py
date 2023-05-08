import asyncio
from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel
from pydantic.generics import GenericModel

_ReturnType = TypeVar("_ReturnType")


class TaskiqResult(GenericModel, Generic[_ReturnType]):
    """Result of a remote task invocation."""

    task_id: str
    is_err: bool
    # Log is a deprecated field. It would be removed in future
    # releases of not, if we find a way to capture logs in async
    # environment.
    log: Optional[str] = None
    return_value: _ReturnType
    execution_time: float


class StreamableResult(BaseModel):
    result: TaskiqResult[Any]
    readiness: asyncio.Event
