import json
import pickle
from functools import partial
from typing import Any, Callable, Dict, Generic, Optional, TypeVar

from pydantic import Field
from typing_extensions import Self

from taskiq.compat import IS_PYDANTIC2
from taskiq.serialization import exception_to_python, prepare_exception

_ReturnType = TypeVar("_ReturnType")


def _json_encoder(value: Any, default: Callable[[Any], Any]) -> Any:
    if isinstance(value, BaseException):
        return prepare_exception(value, json)

    return default(value)


def _json_dumps(value: Any, *, default: Callable[[Any], Any], **kwargs: Any) -> str:
    return json.dumps(value, default=partial(_json_encoder, default=default), **kwargs)


if IS_PYDANTIC2:
    from pydantic import BaseModel, ConfigDict, field_serializer, field_validator

    class TaskiqResult(BaseModel, Generic[_ReturnType]):
        """Result of a remote task invocation."""

        is_err: bool
        # Log is a deprecated field. It would be removed in future
        # releases of not, if we find a way to capture logs in async
        # environment.
        log: Optional[str] = None
        return_value: _ReturnType
        execution_time: float
        labels: Dict[str, Any] = Field(default_factory=dict)

        error: Optional[BaseException] = None

        model_config = ConfigDict(arbitrary_types_allowed=True)

        @field_serializer("error")
        def serialize_error(self, value: BaseException) -> Any:
            """
            Serialize error field.

            :returns: Any
            :param value: exception to serialize.
            """
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
            dict = super().__getstate__()
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


else:
    from pydantic.generics import GenericModel, validator

    class TaskiqResult(GenericModel, Generic[_ReturnType]):  # type: ignore[no-redef]
        """Result of a remote task invocation."""

        is_err: bool
        # Log is a deprecated field. It would be removed in future
        # releases of not, if we find a way to capture logs in async
        # environment.
        log: Optional[str] = None
        return_value: _ReturnType
        execution_time: float
        labels: Dict[str, Any] = Field(default_factory=dict)

        error: Optional[BaseException] = None

        class Config:
            arbitrary_types_allowed = True
            json_dumps = _json_dumps  # type: ignore
            json_loads = json.loads

        def raise_for_error(self) -> "Self":
            """Raise exception if `error`.

            :raises error: task execution exception
            :returns: TaskiqResult
            """
            if self.error is not None:
                raise self.error
            return self

        def __getstate__(self) -> Dict[Any, Any]:
            dict = super().__getstate__()
            vals: Dict[str, Any] = dict["__dict__"]

            if "error" in vals and vals["error"] is not None:
                vals["error"] = prepare_exception(
                    vals["error"],
                    pickle,
                )

            return dict

        @validator("error", pre=True)
        @classmethod
        def _validate_error(cls, value: Any) -> Optional[BaseException]:
            return exception_to_python(value)
