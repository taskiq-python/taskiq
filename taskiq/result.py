import json
import pickle  # noqa: S403
from typing import Any, Dict, Generic, Optional, TypeVar, Union

from pydantic import Field, field_validator, BaseModel, ConfigDict, field_serializer
from typing_extensions import Self

from taskiq.serialization import exception_to_python, prepare_exception, ExceptionRepr

_ReturnType = TypeVar("_ReturnType")


class TaskiqResult(BaseModel, Generic[_ReturnType]):
    """Result of a remote task invocation."""

    is_err: bool
    # Log is a deprecated field. It would be removed in future
    # releases of not, if we find a way to capture logs in async
    # environment.
    log: Optional[str] = None
    return_value: _ReturnType
    execution_time: float
    labels: Dict[str, str] = Field(default_factory=dict)

    error: Optional[BaseException] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_serializer("error")
    def serialize_error(self, value: BaseException):
        if value:
            return prepare_exception(value, json)

        return None

    def raise_for_error(self) -> "Self":
        """Raise exception if `error`.

        :raises error: task execution exception
        :returns: TaskiqResult
        """
        if self.error is not None:
            raise self.error
        return self

    def __getstate__(self) -> Dict[Any, Any]:
        dict = super().__getstate__()  # noqa: WPS125
        vals: Dict[str, Any] = dict["__dict__"]

        if "error" in vals and vals["error"] is not None:
            vals["error"] = prepare_exception(
                vals["error"],
                pickle,
            )

        return dict

    @field_validator("error", mode="before")
    @classmethod
    def _validate_error(cls, value: Any) -> Optional[BaseException]:
        return exception_to_python(value)
