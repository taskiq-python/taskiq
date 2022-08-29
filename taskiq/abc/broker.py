import inspect
import os
import sys
from abc import ABC, abstractmethod
from functools import wraps
from logging import getLogger
from typing import (  # noqa: WPS235
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    TypeVar,
    Union,
    overload,
)
from uuid import uuid4

from typing_extensions import ParamSpec

from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.formatters.json_formatter import JSONFormatter
from taskiq.message import BrokerMessage
from taskiq.result_backends.dummy import DummyResultBackend

if TYPE_CHECKING:
    from taskiq.abc.formatter import TaskiqFormatter
    from taskiq.abc.middleware import TaskiqMiddleware
    from taskiq.abc.result_backend import AsyncResultBackend

_T = TypeVar("_T")  # noqa: WPS111
_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")

logger = getLogger("taskiq")


def default_id_generator() -> str:
    """
    Default task_id generator.

    This function is used to generate id's
    for tasks.

    :return: new task_id.
    """
    return uuid4().hex


class AsyncBroker(ABC):
    """
    Async broker.

    This abstract class must be implemented in order
    to get ability to send tasks to brokers
    in async mode.
    """

    available_tasks: Dict[str, AsyncTaskiqDecoratedTask[Any, Any]] = {}
    is_worker_process: bool = False

    def __init__(
        self,
        result_backend: "Optional[AsyncResultBackend[_T]]" = None,
        task_id_generator: Optional[Callable[[], str]] = None,
    ) -> None:
        if result_backend is None:
            result_backend = DummyResultBackend()
        if task_id_generator is None:
            task_id_generator = default_id_generator
        self.middlewares: "List[TaskiqMiddleware]" = []
        self.result_backend = result_backend
        self.decorator_class = AsyncTaskiqDecoratedTask
        self.formatter: "TaskiqFormatter" = JSONFormatter()
        self.id_generator = task_id_generator

    def add_middlewares(self, middlewares: "List[TaskiqMiddleware]") -> None:
        """
        Add a list of middlewares.

        You should call this method to set middlewares,
        since it saves current broker in all middlewares.

        :param middlewares: list of middlewares.
        """
        for middleware in middlewares:
            middleware.set_broker(self)
            self.middlewares.append(middleware)

    async def startup(self) -> None:
        """Do something when starting broker."""

    async def shutdown(self) -> None:
        """
        Close the broker.

        This method is called,
        when broker is closig.
        """
        for middleware in self.middlewares:
            middleware_shutdown = middleware.shutdown()
            if inspect.isawaitable(middleware_shutdown):
                await middleware_shutdown
        await self.result_backend.shutdown()

    @abstractmethod
    async def kick(
        self,
        message: BrokerMessage,
    ) -> None:
        """
        This method is used to kick tasks out from current program.

        Using this method tasks are sent to
        workers.

        :param message: name of a task.
        """

    @abstractmethod
    async def listen(
        self,
        callback: Callable[[BrokerMessage], Coroutine[Any, Any, None]],
    ) -> None:
        """
        This function listens to new messages and yields them.

        This it the main point for workers.
        This function is used to get new tasks from the network.

        :param callback: function to call when message received.
        :return: nothing.
        """

    @overload
    def task(
        self,
        task_name: Callable[_FuncParams, _ReturnType],
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
        ...

    @overload
    def task(
        self,
        task_name: Optional[str] = None,
        **labels: Union[str, int],
    ) -> Callable[
        [Callable[_FuncParams, _ReturnType]],
        AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
    ]:
        ...

    def task(  # type: ignore[misc]
        self,
        task_name: Optional[str] = None,
        **labels: Union[str, int],
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

        def make_decorated_task(
            inner_labels: Dict[str, Union[str, int]],
            inner_task_name: Optional[str] = None,
        ) -> Callable[
            [Callable[_FuncParams, _ReturnType]],
            AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
        ]:
            def inner(
                func: Callable[_FuncParams, _ReturnType],
            ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
                nonlocal inner_task_name  # noqa: WPS420
                if inner_task_name is None:
                    fmodule = func.__module__
                    if fmodule == "__main__":
                        fmodule = ".".join(  # noqa: WPS220
                            sys.argv[0]
                            .removesuffix(
                                ".py",
                            )
                            .split(
                                os.path.sep,
                            ),
                        )
                    inner_task_name = f"{fmodule}:{func.__name__}"  # noqa: WPS442
                wrapper = wraps(func)

                decorated_task = wrapper(
                    self.decorator_class(
                        broker=self,
                        original_func=func,
                        labels=inner_labels,
                        task_name=inner_task_name,
                    ),
                )

                self.available_tasks[decorated_task.task_name] = decorated_task

                return decorated_task

            return inner

        if callable(task_name):
            # This is an edge case,
            # when decorator called without parameters.
            return make_decorated_task(
                inner_labels=labels or {},
            )(task_name)

        return make_decorated_task(
            inner_task_name=task_name,
            inner_labels=labels or {},
        )
