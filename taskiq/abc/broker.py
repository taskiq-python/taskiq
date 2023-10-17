import os
import sys
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from functools import wraps
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    ClassVar,
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
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.acks import AckableMessage
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.events import TaskiqEvents
from taskiq.formatters.proxy_formatter import ProxyFormatter
from taskiq.message import BrokerMessage
from taskiq.result_backends.dummy import DummyResultBackend
from taskiq.serializers.json_serializer import JSONSerializer
from taskiq.state import TaskiqState
from taskiq.utils import maybe_awaitable, remove_suffix
from taskiq.warnings import TaskiqDeprecationWarning

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.formatter import TaskiqFormatter
    from taskiq.abc.result_backend import AsyncResultBackend

_T = TypeVar("_T")
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

    global_task_registry: ClassVar[Dict[str, AsyncTaskiqDecoratedTask[Any, Any]]] = {}

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
                stacklevel=2,
            )
        if task_id_generator is None:
            task_id_generator = default_id_generator
        else:
            warnings.warn(
                "Setting id generator with constructor is deprecated. "
                "Please use `with_id_generator` instead.",
                TaskiqDeprecationWarning,
                stacklevel=2,
            )
        self.middlewares: "List[TaskiqMiddleware]" = []
        self.result_backend = result_backend
        self.decorator_class = AsyncTaskiqDecoratedTask
        self.serializer: TaskiqSerializer = JSONSerializer()
        self.formatter: "TaskiqFormatter" = ProxyFormatter(self)
        self.id_generator = task_id_generator
        self.local_task_registry: Dict[str, AsyncTaskiqDecoratedTask[Any, Any]] = {}
        # Every event has a list of handlers.
        # Every handler is a function which takes state as a first argument.
        # And handler can be either sync or async.
        self.event_handlers: DefaultDict[
            TaskiqEvents,
            List[Callable[[TaskiqState], Optional[Awaitable[None]]]],
        ] = defaultdict(list)
        self.state = TaskiqState()
        self.custom_dependency_context: Dict[Any, Any] = {}
        # True only if broker runs in worker process.
        self.is_worker_process: bool = False
        # True only if broker runs in scheduler process.
        self.is_scheduler_process: bool = False

    def find_task(self, task_name: str) -> Optional[AsyncTaskiqDecoratedTask[Any, Any]]:
        """
        Returns task by name.

        This method should be used to get task by name.
        Instead of accessing `available_tasks` or `local_available_tasks` directly.

        It searches task by name in dict of tasks that
        were registered for this broker directly.
        If it fails, it checks global dict of all available tasks.

        :param task_name: name of a task.
        :returns: found task or None.
        """
        return self.local_task_registry.get(
            task_name,
        ) or self.global_task_registry.get(
            task_name,
        )

    def get_all_tasks(self) -> Dict[str, AsyncTaskiqDecoratedTask[Any, Any]]:
        """
        Method to fetch all tasks available in broker.

        This method returns all tasks, globally and locally
        available in broker. With local tasks having higher priority.

        So, if you have two tasks with the same name,
        one registered in global registry and one registered
        in local registry, then local task will be returned.

        :return: dict of all tasks. Keys are task names, values are tasks.
        """
        return {**self.global_task_registry, **self.local_task_registry}

    def add_dependency_context(self, new_ctx: Dict[Any, Any]) -> None:
        """
        Add first-level dependencies.

        Provided dict will be used to inject new dependencies
        in all dependency graph contexts.

        :param new_ctx: Additional context values for dependency injection.
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

        for middleware in self.middlewares:
            if middleware.__class__.startup != TaskiqMiddleware.startup:
                await maybe_awaitable(middleware.startup())

        await self.result_backend.startup()

    async def shutdown(self) -> None:
        """
        Close the broker.

        This method is called,
        when broker is closing.
        """
        event = TaskiqEvents.CLIENT_SHUTDOWN
        if self.is_worker_process:
            event = TaskiqEvents.WORKER_SHUTDOWN

        # Call all shutdown events.
        for handler in self.event_handlers[event]:
            await maybe_awaitable(handler(self.state))

        for middleware in self.middlewares:
            if middleware.__class__.shutdown != TaskiqMiddleware.shutdown:
                await maybe_awaitable(middleware.shutdown)

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
    def listen(self) -> AsyncGenerator[Union[bytes, AckableMessage], None]:
        """
        This function listens to new messages and yields them.

        This it the main point for workers.
        This function is used to get new tasks from the network.

        If your broker support acknowledgement, then you
        should wrap your message in AckableMessage dataclass.

        If your messages was wrapped in AckableMessage dataclass,
        taskiq will call ack when finish processing message.

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
                nonlocal inner_task_name
                if inner_task_name is None:
                    fmodule = func.__module__
                    if fmodule == "__main__":  # pragma: no cover
                        fmodule = ".".join(
                            remove_suffix(sys.argv[0], ".py").split(
                                os.path.sep,
                            ),
                        )
                    fname = func.__name__
                    if fname == "<lambda>":
                        fname = f"lambda_{uuid4().hex}"
                    inner_task_name = f"{fmodule}:{fname}"
                wrapper = wraps(func)

                decorated_task = wrapper(
                    self.decorator_class(
                        broker=self,
                        original_func=func,
                        labels=inner_labels,
                        task_name=inner_task_name,
                    ),
                )

                self._register_task(decorated_task.task_name, decorated_task)

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

    def register_task(
        self,
        func: Callable[_FuncParams, _ReturnType],
        task_name: Optional[str] = None,
        **labels: Any,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
        """
        API for registering tasks programmatically.

        This function is basically the same as `task` decorator,
        but it doesn't decorate function, it just registers it
        and returns AsyncTaskiqDecoratedTask object, that can
        be called later.

        :param func: function to register.
        :param task_name: custom name of a task, defaults to qualified function's name.
        :param labels: some addition labels for task.

        :returns: registered task.
        """
        return self.task(task_name=task_name, **labels)(func)

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

    def with_serializer(
        self,
        serializer: TaskiqSerializer,
    ) -> "Self":  # pragma: no cover
        """
        Set a new serializer and return an updated broker.

        :param serializer: new serializer.
        :return: self
        """
        self.serializer = serializer
        return self

    def with_formatter(self, formatter: "TaskiqFormatter") -> "Self":
        """
        Set new formatter and return an updated broker.

        :param formatter: new formatter.
        :return: self
        """
        self.formatter = formatter
        return self

    def _register_task(
        self,
        task_name: str,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> None:
        """
        Mehtod is used to register tasks.

        By default we register tasks in local task registry.
        But this behaviour can be changed in subclasses.

        :param task_name: Name of a task.
        :param task: Decorated task.
        """
        self.local_task_registry[task_name] = task
