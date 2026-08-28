import os
import sys
from collections.abc import Callable
from copy import copy
from functools import wraps
from typing import TYPE_CHECKING, Any, get_type_hints
from uuid import uuid4

from typing_extensions import ParamSpec, Self, TypeVar

from taskiq.decor import AsyncTaskiqDecoratedTask

if TYPE_CHECKING:
    from taskiq.abc.broker import AsyncBroker

_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")


class TaskiqTaskGenerator:
    """Class used for task generation."""

    def __init__(self) -> None:
        self._labels: dict[str, Any] = {}
        self._name: str | None = None
        self._broker: "AsyncBroker | None" = None

    def name(self, name: str) -> Self:
        """Set task name."""
        inst = copy(self)
        inst._name = name  # noqa: SLF001
        return inst

    def labels(self, **labels: Any) -> Self:
        """Set task's static labels."""
        inst = copy(self)
        inst._labels = labels  # noqa: SLF001
        return inst

    def broker(self, broker: "AsyncBroker") -> Self:
        """Set a broker."""
        inst = copy(self)
        inst._broker = broker  # noqa: SLF001
        return inst

    @classmethod
    def make_task(
        cls,
        task_name: str,
        broker: "AsyncBroker | None",
        original_func: Callable[_FuncParams, _ReturnType],
        labels: dict[str, Any],
        return_type: type[_ReturnType] | None = None,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
        """
        Create a task out of given inputs.

        This method can be overridden to create custom task classes
        with custom arguments and logic.
        """
        return AsyncTaskiqDecoratedTask(
            broker=broker,
            task_name=task_name,
            original_func=original_func,
            labels=labels,
            return_type=return_type,
        )

    def __call__(
        self,
        func: Callable[_FuncParams, _ReturnType],
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
        """
        Make a decorated task.

        This function is the main point for creating a task
        from a raw function.
        """
        task_name = self._name
        if task_name is None:
            fmodule = func.__module__
            if fmodule == "__main__":  # pragma: no cover
                fmodule = ".".join(
                    os.path.normpath(sys.argv[0])
                    .removesuffix(".py")
                    .split(os.path.sep),
                )
            fname = func.__name__
            if fname == "<lambda>":
                fname = f"lambda_{uuid4().hex}"
            task_name = f"{fmodule}:{fname}"
        wrapper = wraps(func)

        sign = get_type_hints(func)
        return_type = None
        if "return" in sign:
            return_type = sign["return"]

        return wrapper(
            self.make_task(
                original_func=func,
                labels=self._labels,
                task_name=task_name,
                broker=self._broker,
                return_type=return_type,  # type: ignore
            ),
        )


task_gen = TaskiqTaskGenerator()
