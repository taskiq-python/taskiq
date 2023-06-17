import json
import pickle  # noqa: S403
from functools import partial
from typing import Any, Callable, Dict, Generic, Optional, TypeVar

from pydantic import Field, validator
from pydantic.generics import GenericModel
from typing_extensions import Self

from taskiq.serialization import exception_to_python, prepare_exception

_ReturnType = TypeVar("_ReturnType")


def _json_encoder(value: Any, default: Callable[[Any], Any]) -> Any:
    if isinstance(value, BaseException):
        return prepare_exception(value, json)

    return default(value)


def _json_dumps(value: Any, *, default: Callable[[Any], Any], **kwargs: Any) -> str:
    return json.dumps(value, default=partial(_json_encoder, default=default), **kwargs)


class TaskiqResult(GenericModel, Generic[_ReturnType]):
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
        dict = super().__getstate__()  # noqa: WPS125
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
