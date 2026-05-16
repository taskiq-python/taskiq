import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, ParamSpec, TypeVar, overload

from taskiq.message import TaskiqMessage

__all__ = ("TaskDefinition", "task_builder")

_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")


@dataclass(frozen=True, slots=True)
class TaskDefinition(Generic[_FuncParams, _ReturnType]):
    """Unbound task declaration that can be registered later."""

    task_name: str
    original_func: Callable[_FuncParams, _ReturnType]
    labels: dict[str, Any] = field(default_factory=dict)
    return_type: type[_ReturnType] | None = None

    def __call__(
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> _ReturnType:
        """Call original function directly."""
        return self.original_func(*args, **kwargs)

    async def call(
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> _ReturnType:
        """Execute original function in the current process."""
        result = self.original_func(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def message(
        self,
        task_id: str,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> TaskiqMessage:
        """Build a TaskiqMessage without binding this definition to a router."""
        return TaskiqMessage(
            task_id=task_id,
            task_name=self.task_name,
            labels=dict(self.labels),
            args=list(args),
            kwargs=dict(kwargs),
        )


@overload
def task_builder(
    task_name: Callable[_FuncParams, _ReturnType],
    **labels: Any,
) -> TaskDefinition[_FuncParams, _ReturnType]: ...


@overload
def task_builder(
    task_name: str | None = None,
    **labels: Any,
) -> Callable[
    [Callable[_FuncParams, _ReturnType]],
    TaskDefinition[_FuncParams, _ReturnType],
]: ...


def task_builder(
    task_name: str | Callable[_FuncParams, _ReturnType] | None = None,
    **labels: Any,
) -> Any:
    """Build an unbound task definition.

    This decorator is intended for library/package tasks that should be
    registered by the final application.
    """

    def build(
        func: Callable[_FuncParams, _ReturnType],
    ) -> TaskDefinition[_FuncParams, _ReturnType]:
        real_task_name = task_name
        if real_task_name is None or callable(real_task_name):
            real_task_name = f"{func.__module__}:{func.__name__}"
        return_type = None
        signature = inspect.signature(func)
        if signature.return_annotation is not inspect.Signature.empty:
            return_type = signature.return_annotation
        return TaskDefinition(
            task_name=real_task_name,
            original_func=func,
            labels=dict(labels),
            return_type=return_type,
        )

    if callable(task_name):
        function = task_name
        task_name = None
        return build(function)

    return build
