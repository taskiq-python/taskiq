from abc import ABC, abstractmethod
from functools import wraps
from logging import getLogger
from typing import Any, AsyncGenerator, Callable, Dict, Optional, Union, overload

from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.message import TaskiqMessage
from taskiq.result_backends.dummy import DummyResultBackend
from taskiq.types_helpers import T_, FuncParams_, ReturnType_

logger = getLogger("taskiq")


class AsyncBroker(ABC):
    """
    Async broker.

    This abstract class must be implemented in order
    to get ability to send tasks to brokers
    in async mode.
    """

    available_tasks: Dict[str, AsyncTaskiqDecoratedTask[Any, Any]] = {}

    def __init__(
        self,
        result_backend: Optional[AsyncResultBackend[T_]] = None,
    ) -> None:
        if result_backend is None:
            result_backend = DummyResultBackend()
        self.result_backend = result_backend
        self.is_worker_process = False

    async def startup(self) -> None:
        """Do something when starting broker."""

    async def shutdown(self) -> None:
        """
        Close the broker.

        This method is called,
        when broker is closig.
        """

    @abstractmethod
    async def kick(
        self,
        message: TaskiqMessage,
    ) -> None:
        """
        This method is used to kick tasks out from current program.

        Using this method tasks are sent to
        workers.

        :param message: name of a task.
        """

    @abstractmethod
    def listen(self) -> AsyncGenerator[TaskiqMessage, None]:
        """
        This function listens to new messages and yields them.

        This it the main point for workers.
        This function is used to get new tasks from the network.

        :yields: taskiq messages.
        :return: nothing.
        """

    @overload
    def task(
        self,
        task_name: Callable[FuncParams_, ReturnType_],
    ) -> AsyncTaskiqDecoratedTask[FuncParams_, ReturnType_]:
        ...

    @overload
    def task(
        self,
        task_name: Optional[str] = None,
        **labels: Union[str, int],
    ) -> Callable[
        [Callable[FuncParams_, ReturnType_]],
        AsyncTaskiqDecoratedTask[FuncParams_, ReturnType_],
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
            [Callable[FuncParams_, ReturnType_]],
            AsyncTaskiqDecoratedTask[FuncParams_, ReturnType_],
        ]:
            def inner(
                func: Callable[FuncParams_, ReturnType_],
            ) -> AsyncTaskiqDecoratedTask[FuncParams_, ReturnType_]:
                nonlocal inner_task_name  # noqa: WPS420
                if inner_task_name is None:
                    inner_task_name = (  # noqa: WPS442
                        f"{func.__module__}:{func.__name__}"
                    )
                wrapper = wraps(func)

                decorated_task = wrapper(
                    AsyncTaskiqDecoratedTask(
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
