import builtins
import json
import pickle
import sys
from collections.abc import Callable
from functools import partial
from typing import Any, Generic, TypeVar

from pydantic import Field

from taskiq.compat import IS_PYDANTIC2
from taskiq.serialization import exception_to_python, prepare_exception

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

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
        log: str | None = None
        return_value: _ReturnType
        execution_time: float
        labels: dict[str, Any] = Field(default_factory=dict)

        error: BaseException | None = None

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

        def __getstate__(self) -> dict[Any, Any]:
            dict = super().__getstate__()
            vals: builtins.dict[str, Any] = dict["__dict__"]

            if "error" in vals and vals["error"] is not None:
                vals["error"] = prepare_exception(
                    vals["error"],
                    pickle,
                )

            return dict

        @field_validator("error", mode="before")
        @classmethod
        def _validate_error(cls, value: Any) -> BaseException | None:
            return exception_to_python(value)

else:
    from pydantic import validator
    from pydantic.generics import GenericModel

    class TaskiqResult(GenericModel, Generic[_ReturnType]):  # type: ignore[no-redef]
        """Result of a remote task invocation."""

        is_err: bool
        # Log is a deprecated field. It would be removed in future
        # releases of not, if we find a way to capture logs in async
        # environment.
        log: str | None = None
        return_value: _ReturnType
        execution_time: float
        labels: dict[str, Any] = Field(default_factory=dict)

        error: BaseException | None = None

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

        def __getstate__(self) -> dict[Any, Any]:
            dict = super().__getstate__()
            vals: builtins.dict[str, Any] = dict["__dict__"]

            if "error" in vals and vals["error"] is not None:
                vals["error"] = prepare_exception(
                    vals["error"],
                    pickle,
                )

            return dict

        @validator("error", pre=True)
        @classmethod
        def _validate_error(cls, value: Any) -> BaseException | None:
            return exception_to_python(value)
