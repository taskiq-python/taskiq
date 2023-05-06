import os
import sys
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from functools import wraps
from logging import getLogger
from typing import (  # noqa: WPS235
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    List,
    Optional,
    TypeVar,
    Union,
    overload,
)
from uuid import uuid4

from typing_extensions import ParamSpec, Self, TypeAlias

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.events import TaskiqEvents
from taskiq.formatters.json_formatter import JSONFormatter
from taskiq.message import BrokerMessage
from taskiq.result_backends.dummy import DummyResultBackend
from taskiq.state import TaskiqState
from taskiq.utils import maybe_awaitable, remove_suffix
from taskiq.warnings import TaskiqDeprecationWarning

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.formatter import TaskiqFormatter
    from taskiq.abc.result_backend import AsyncResultBackend

_T = TypeVar("_T")  # noqa: WPS111
_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")

EventHandler: TypeAlias = Callable[[TaskiqState], Optional[Awaitable[None]]]

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
        else:
            warnings.warn(
                "Setting result backend with constructor is deprecated. "
                "Please use `with_result_backend` instead.",
                TaskiqDeprecationWarning,
            )
        if task_id_generator is None:
            task_id_generator = default_id_generator
        else:
            warnings.warn(
                "Setting id generator with constructor is deprecated. "
                "Please use `with_id_generator` instead.",
                TaskiqDeprecationWarning,
            )
        self.middlewares: "List[TaskiqMiddleware]" = []
        self.result_backend = result_backend
        self.decorator_class = AsyncTaskiqDecoratedTask
        self.formatter: "TaskiqFormatter" = JSONFormatter()
        self.id_generator = task_id_generator
        # Every event has a list of handlers.
        # Every handler is a function which takes state as a first argument.
        # And handler can be either sync or async.
        self.event_handlers: DefaultDict[
            TaskiqEvents,
            List[Callable[[TaskiqState], Optional[Awaitable[None]]]],
        ] = defaultdict(list)
        self.state = TaskiqState()
        self.custom_dependency_context: Dict[Any, Any] = {}

    def add_dependency_context(self, new_ctx: Dict[Any, Any]) -> None:
        """
        Add first-level dependencies.

        Provided dict will be used to inject new dependencies
        in all dependency graph contexts.

        :param new_ctx: Additional context values for dependnecy injection.
        """
        self.custom_dependency_context.update(new_ctx)

    def add_middlewares(self, *middlewares: "TaskiqMiddleware") -> None:
        """
        Add a list of middlewares.

        You should call this method to set middlewares,
        since it saves current broker in all middlewares.

        :param middlewares: list of middlewares.
        """
        for middleware in middlewares:
            if not isinstance(middleware, TaskiqMiddleware):
                logger.warning(
                    f"Middleware {middleware} is not an instance of TaskiqMiddleware. "
                    "Skipping...",
                )
                continue
            middleware.set_broker(self)
            self.middlewares.append(middleware)

    async def startup(self) -> None:
        """Do something when starting broker."""
        event = TaskiqEvents.CLIENT_STARTUP
        if self.is_worker_process:
            event = TaskiqEvents.WORKER_STARTUP

        for handler in self.event_handlers[event]:
            await maybe_awaitable(handler(self.state))

        await self.result_backend.startup()

    async def shutdown(self) -> None:
        """
        Close the broker.

        This method is called,
        when broker is closig.
        """
        event = TaskiqEvents.CLIENT_SHUTDOWN
        if self.is_worker_process:
            event = TaskiqEvents.WORKER_SHUTDOWN

        # Call all shutdown events.
        for handler in self.event_handlers[event]:
            await maybe_awaitable(handler(self.state))

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

        You don't need to send broker message. It's helper for brokers,
        please send only bytes from message.message.

        :param message: name of a task.
        """

    @abstractmethod
    def listen(self) -> AsyncGenerator[bytes, None]:
        """
        This function listens to new messages and yields them.

        This it the main point for workers.
        This function is used to get new tasks from the network.

        :yield: incoming messages.
        :return: nothing.
        """

    @overload
    def task(
        self,
        task_name: Callable[_FuncParams, _ReturnType],
        **labels: Any,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:  # pragma: no cover
        ...

    @overload
    def task(
        self,
        task_name: Optional[str] = None,
        **labels: Any,
    ) -> Callable[
        [Callable[_FuncParams, _ReturnType]],
        AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
    ]:  # pragma: no cover
        ...

    def task(  # type: ignore[misc]
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
                    if fmodule == "__main__":  # pragma: no cover
                        fmodule = ".".join(
                            remove_suffix(sys.argv[0], ".py").split(
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

    def on_event(self, *events: TaskiqEvents) -> Callable[[EventHandler], EventHandler]:
        """
        Adds event handler.

        This function adds function to call when event occurs.

        :param events: events to react to.
        :return: a decorator function.
        """

        def handler(function: EventHandler) -> EventHandler:
            for event in events:
                self.event_handlers[event].append(function)
            return function

        return handler

    def add_event_handler(
        self,
        event: TaskiqEvents,
        handler: EventHandler,
    ) -> None:  # pragma: no cover
        """
        Adds event handler.

        this function is the same as on_event.

        >>> broker.add_event_handler(TaskiqEvents.WORKER_STARTUP, my_startup)

        if similar to:

        >>> @broker.on_event(TaskiqEvents.WORKER_STARTUP)
        >>> async def my_startup(context: Context) -> None:
        >>>    ...

        :param event: Event to react to.
        :param handler: handler to call when event is started.
        """
        self.event_handlers[event].append(handler)

    def with_result_backend(
        self,
        result_backend: "AsyncResultBackend[_T]",
    ) -> "Self":  # pragma: no cover
        """
        Set a result backend and return updated broker.

        :param result_backend: new result backend.
        :return: self
        """
        self.result_backend = result_backend
        return self

    def with_id_generator(
        self,
        task_id_generator: Callable[[], str],
    ) -> "Self":  # pragma: no cover
        """
        Set a new Id generator and return an updated broker.

        :param task_id_generator: new generator function.
        :return: self
        """
        self.id_generator = task_id_generator
        return self

    def with_middlewares(
        self,
        *middlewares: "TaskiqMiddleware",
    ) -> "Self":  # pragma: no cover
        """
        Add middewares to broker.

        This method adds new middlewares to broker
        and returns and updated broker.

        :param middlewares: list of middlewares.
        :return: self
        """
        for middleware in middlewares:
            if not isinstance(middleware, TaskiqMiddleware):
                logger.warning(
                    f"Middleware {middleware} is not an instance of TaskiqMiddleware. "
                    "Skipping...",
                )
                continue
            middleware.set_broker(self)
            self.middlewares.append(middleware)
        return self

    def with_event_handlers(
        self,
        event: TaskiqEvents,
        *handlers: EventHandler,
    ) -> "Self":  # pragma: no cover
        """
        Set new event handlers.

        It takes an event to handle, list of handlers
        and sets it to broker, returning an updated broker.

        :param event: event to handle.
        :param handlers: event handlers.
        :return: self
        """
        self.event_handlers[event].extend(handlers)
        return self
