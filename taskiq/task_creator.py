import os
import sys
import warnings
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
    TypeVar,
    overload,
)
from uuid import uuid4

from typing_extensions import ParamSpec, Self

from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.utils import remove_suffix

if TYPE_CHECKING:
    from taskiq.abc.broker import AsyncBroker


_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")


class BaseTaskCreator:
    """
    Base class for task creator.

    Instances of this class are used to make tasks out of the given functions.
    """

    def __init__(self, broker: "AsyncBroker") -> None:
        self._broker = broker
        self._task_name: Optional[str] = None
        self._labels: Dict[str, Any] = {}

    def name(self, name: str) -> Self:
        """Assign custom name to the task."""
        self._task_name = name
        return self

    def labels(self, **labels: Any) -> Self:
        """Assign custom labels to the task."""
        self._labels = labels
        return self

    def make_task(
        self,
        task_name: str,
        func: Callable[_FuncParams, _ReturnType],
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
        """Make a task from the given function."""
        return AsyncTaskiqDecoratedTask(
            broker=self._broker,
            original_func=func,
            labels=self._labels,
            task_name=task_name,
        )

    @overload
    def __call__(
        self,
        task_name: Callable[_FuncParams, _ReturnType],
        **labels: Any,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:  # pragma: no cover
        ...

    @overload
    def __call__(
        self,
        task_name: Optional[str] = None,
        **labels: Any,
    ) -> Callable[
        [Callable[_FuncParams, _ReturnType]],
        AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
    ]:  # pragma: no cover
        ...

    def __call__(  # type: ignore[misc]
        self,
        task_name: Optional[str] = None,
        **labels: Any,
    ) -> Any:
        """
        Decorator that turns function into a task.

        This decorator converts function to
        a `TaskiqDecoratedTask` object.

        This object can be called as a usual function,
        because it uses decorated function in it's __call__
        method.

        !! You have to use it with parentheses in order to
        get autocompletion. Like this:

        >>> @task()
        >>> def my_func():
        >>>     ...

        :param task_name: custom name of a task, defaults to decorated function's name.
        :param labels: some addition labels for task.

        :returns: decorator function or AsyncTaskiqDecoratedTask.
        """
        if task_name is not None and isinstance(task_name, str):
            warnings.warn(
                "Using task_name is deprecated, @broker.task.name('name') instead",
                DeprecationWarning,
                stacklevel=2,
            )
            self._task_name = task_name
        if labels:
            warnings.warn(
                "Using labels is deprecated, @broker.task.labels(**labels) instead",
                DeprecationWarning,
                stacklevel=2,
            )
            self._labels.update(labels)

        def make_decorated_task() -> Callable[
            [Callable[_FuncParams, _ReturnType]],
            AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
        ]:
            def inner(
                func: Callable[_FuncParams, _ReturnType],
            ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
                inner_task_name = self._task_name
                if inner_task_name is None:
                    fmodule = func.__module__
                    if fmodule == "__main__":  # pragma: no cover
                        fmodule = ".".join(
                            remove_suffix(sys.argv[0], ".py").split(os.path.sep),
                        )
                    fname = func.__name__
                    if fname == "<lambda>":
                        fname = f"lambda_{uuid4().hex}"
                    inner_task_name = f"{fmodule}:{fname}"

                wrapper = wraps(func)
                decorated_task = wrapper(
                    self.make_task(
                        task_name=inner_task_name,
                        func=func,
                    ),
                )

                # We need these ignored lines because
                # `wrap` function copies __annotations__,
                # therefore mypy thinks that decorated_task
                # is still an instance of the original function.
                self._broker._register_task(  # noqa: SLF001
                    decorated_task.task_name,  # type: ignore
                    decorated_task,  # type: ignore
                )

                return decorated_task  # type: ignore

            return inner

        if callable(task_name):
            # This is an edge case,
            # when decorator called without parameters.
            return make_decorated_task()(task_name)

        return make_decorated_task()
