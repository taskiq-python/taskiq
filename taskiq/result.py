from typing import Generic, Optional

from pydantic.generics import GenericModel

from taskiq.types_helpers import ReturnType_


class TaskiqResult(GenericModel, Generic[ReturnType_]):
    """Result of a remote task invocation."""

    is_err: bool
    log: Optional[str]
    return_value: ReturnType_
    execution_time: float
