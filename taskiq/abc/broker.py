from abc import ABC, abstractmethod
from functools import wraps
from logging import getLogger
from typing import (  # noqa: WPS235
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Generic,
    Optional,
    Set,
    TypeVar,
    Union,
    overload,
)
from uuid import uuid4

from pydantic import BaseModel
from typing_extensions import ParamSpec

from taskiq.abc.result_backend import AsyncResultBackend, AsyncTaskiqTask
from taskiq.message import TaskiqMessage
from taskiq.result_backends.dummy import DummyResultBackend

logger = getLogger("taskiq")

_T = TypeVar("_T")  # noqa: WPS111
_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")


class AsyncTaskiqDecoratedTask(Generic[_FuncParams, _ReturnType]):
    """
    Class for all task functions.

    When function is decorated
    with the `task` decorator, it
    will return an instance of this class.

    This class parametrized with original function's
    arguments types and a return type.

    This class has kiq method which is used
    to kick tasks out of this thread and send them to
    current broker.
    """

    def __init__(
        self,
        broker: "AsyncBroker",
        task_name: str,
        original_func: Callable[_FuncParams, _ReturnType],
        labels: Dict[str, Any],
    ) -> None:
        self.broker = broker
        self.task_name = task_name
        self.original_func = original_func
        self.labels = labels

    # Docs for this method are ommited in order to help
    # your IDE resolve correct docs for it.
    def __call__(  # noqa: D102
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> _ReturnType:
        return self.original_func(*args, **kwargs)

    async def kiq(
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncTaskiqTask[_ReturnType]:
        """
        This method sends function call over the network.

        It gets current broker and calls it's kick method,
        returning what it returns.

        :param args: function's arguments.
        :param kwargs: function's key word arguments.

        :returns: taskiq task.
        """
        logger.debug(
            f"Kicking {self.task_name} with args={args} and kwargs={kwargs}.",
        )
        message = self._prepare_message(*args, **kwargs)
        await self.broker.kick(message)
        return self.broker.result_backend.generate_task(message.task_id)  # type: ignore

    def __repr__(self) -> str:
        return f"AsyncTaskiqDecoratedTask({self.task_name})"

    @classmethod
    def _prepare_arg(cls, arg: Any) -> Any:
        if isinstance(arg, BaseModel):
            arg = arg.dict()
        return arg

    def _prepare_message(  # noqa: WPS210
        self,
        *args: Any,
        **kwargs: Any,
    ) -> TaskiqMessage:
        """
        Create a message from args and kwargs.

        :param args: function's args.
        :param kwargs: function's kwargs.
        :return: constructed message.
        """
        formatted_args = []
        formatted_kwargs = {}
        for arg in args:
            formatted_args.append(self._prepare_arg(arg))
        for kwarg_name, kwarg_val in kwargs.items():
            formatted_kwargs[kwarg_name] = self._prepare_arg(kwarg_val)

        task_id = uuid4().hex

        return TaskiqMessage(
            task_id=task_id,
            task_name=self.task_name,
            meta=self.labels,
            args=formatted_args,
            kwargs=formatted_kwargs,
        )


class AsyncBroker(ABC):
    """
    Async broker.

    This abstract class must be implemented in order
    to get ability to send tasks to brokers
    in async mode.
    """

    def __init__(
        self,
        result_backend: Optional[AsyncResultBackend[_T]] = None,
    ) -> None:
        if result_backend is None:
            result_backend = DummyResultBackend()
        self.result_backend = result_backend
        self.is_worker_process = False
        self._related_tasks: Set[AsyncTaskiqDecoratedTask[..., Any]] = set()

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

                # Adds this task to the list of tasks.
                # This option is used by workers.
                if self.is_worker_process:
                    self._related_tasks.add(decorated_task)  # type: ignore

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
