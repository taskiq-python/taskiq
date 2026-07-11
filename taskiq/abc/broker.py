import os
import sys
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import AsyncGenerator, Awaitable, Callable
from functools import wraps
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    ParamSpec,
    TypeAlias,
    TypeVar,
    get_type_hints,
    overload,
)
from uuid import uuid4

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.acks import AckableMessage
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.events import TaskiqEvents
from taskiq.exceptions import TaskBrokerMismatchError
from taskiq.flow import FlowProtocol
from taskiq.formatters.proxy_formatter import ProxyFormatter
from taskiq.message import BrokerMessage
from taskiq.result_backends.dummy import DummyResultBackend
from taskiq.router import TaskiqRouter
from taskiq.serializers.json_serializer import JSONSerializer
from taskiq.state import TaskiqState
from taskiq.task_builder import TaskDefinition, _get_task_binding_function
from taskiq.utils import maybe_awaitable
from taskiq.warnings import TaskiqDeprecationWarning

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.formatter import TaskiqFormatter
    from taskiq.abc.result_backend import AsyncResultBackend

_T = TypeVar("_T")
_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")

EventHandler: TypeAlias = Callable[[TaskiqState], Awaitable[None] | None]

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

    global_task_registry: ClassVar[dict[str, AsyncTaskiqDecoratedTask[Any, Any]]] = {}

    def __init__(
        self,
        result_backend: "AsyncResultBackend[_T] | None" = None,
        task_id_generator: Callable[[], str] | None = None,
        *,
        router: TaskiqRouter | None = None,
        broker_name: str | None = None,
        default_flow: FlowProtocol | None = None,
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
        self.middlewares: list[TaskiqMiddleware] = []
        self.result_backend = result_backend
        self.decorator_class = AsyncTaskiqDecoratedTask
        self.serializer: TaskiqSerializer = JSONSerializer()
        self.formatter: TaskiqFormatter = ProxyFormatter(self)
        self.id_generator = task_id_generator
        self.router = router or TaskiqRouter()
        self.default_flow = default_flow
        self.broker_name = self.router.set_broker(
            self,
            name=broker_name,
            default_flow=default_flow,
        )
        self.local_task_registry: dict[str, AsyncTaskiqDecoratedTask[Any, Any]] = {}
        # Every event has a list of handlers.
        # Every handler is a function which takes state as a first argument.
        # And handler can be either sync or async.
        self.event_handlers: defaultdict[
            TaskiqEvents,
            list[Callable[[TaskiqState], Awaitable[None] | None]],
        ] = defaultdict(list)
        self.state = TaskiqState()
        self.custom_dependency_context: dict[Any, Any] = {}
        self.dependency_overrides: dict[Any, Any] = {}
        # True only if broker runs in worker process.
        self.is_worker_process = False
        # True only if broker runs in scheduler process.
        self.is_scheduler_process = False

    def find_task(self, task_name: str) -> AsyncTaskiqDecoratedTask[Any, Any] | None:
        """
        Returns task by name.

        This method should be used to get task by name.
        Instead of accessing `available_tasks` or `local_available_tasks` directly.

        It searches task by name in dict of tasks that
        were registered for this broker directly.
        If it fails, it checks tasks registered in the broker's router,
        then global dict of all available tasks.

        :param task_name: name of a task.
        :returns: found task or None.
        """
        return (
            self.local_task_registry.get(
                task_name,
            )
            or self.router.find_task(task_name)
            or self.global_task_registry.get(
                task_name,
            )
        )

    def get_all_tasks(self) -> dict[str, AsyncTaskiqDecoratedTask[Any, Any]]:
        """
        Method to fetch all tasks available in broker.

        This method returns all tasks globally, through the broker's router
        and locally available in broker. With local tasks having higher
        priority.

        So, if you have two tasks with the same name,
        one registered in global registry and one registered
        in local registry, then local task will be returned.

        :return: dict of all tasks. Keys are task names, values are tasks.
        """
        return {
            **self.global_task_registry,
            **self.router.get_all_tasks(),
            **self.local_task_registry,
        }

    def get_subscribed_flows(self) -> tuple[FlowProtocol, ...]:
        """
        Return flows this broker should subscribe to.

        Existing brokers can keep their current `listen` implementation. New
        flow-aware brokers may use this method to configure queue, topic,
        stream or subject subscriptions from router-owned rules.
        """
        return self.router.get_broker_flows(self)

    def add_dependency_context(self, new_ctx: dict[Any, Any]) -> None:
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
        for event in self._get_startup_events():
            for handler in self.event_handlers[event]:
                await maybe_awaitable(handler(self.state))

        for middleware in self.middlewares:
            if middleware.__class__.startup != TaskiqMiddleware.startup:
                await maybe_awaitable(middleware.startup())

        await self.result_backend.startup()

    def _get_startup_events(self) -> tuple[TaskiqEvents, ...]:
        """Return event phases owned by this broker startup."""
        if self.is_worker_process:
            return (TaskiqEvents.WORKER_STARTUP,)
        return (TaskiqEvents.CLIENT_STARTUP,)

    async def shutdown(self) -> None:
        """
        Close the broker.

        This method is called,
        when broker is closing.
        """
        shutdown_error: BaseException | None = None

        for event in self._get_shutdown_events():
            for handler in self.event_handlers[event]:
                try:
                    await maybe_awaitable(handler(self.state))
                except BaseException as exc:
                    shutdown_error = self._remember_shutdown_error(
                        shutdown_error,
                        exc,
                    )

        for middleware in self.middlewares:
            if middleware.__class__.shutdown != TaskiqMiddleware.shutdown:
                try:
                    await maybe_awaitable(middleware.shutdown())
                except BaseException as exc:
                    shutdown_error = self._remember_shutdown_error(
                        shutdown_error,
                        exc,
                    )

        try:
            await self.result_backend.shutdown()
        except BaseException as exc:
            shutdown_error = self._remember_shutdown_error(shutdown_error, exc)

        if shutdown_error is not None:
            raise shutdown_error

    def _get_shutdown_events(self) -> tuple[TaskiqEvents, ...]:
        """Return event phases owned by this broker shutdown."""
        if self.is_worker_process:
            return (TaskiqEvents.WORKER_SHUTDOWN,)
        return (TaskiqEvents.CLIENT_SHUTDOWN,)

    @staticmethod
    def _remember_shutdown_error(
        first_error: BaseException | None,
        current_error: BaseException,
    ) -> BaseException:
        """Keep the first shutdown failure while allowing cleanup to continue."""
        if first_error is None:
            return current_error
        logger.error(
            "Additional error while shutting down broker resources.",
            exc_info=current_error,
        )
        return first_error

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

    async def kick_to_flow(
        self,
        message: BrokerMessage,
        flow: FlowProtocol | None = None,
    ) -> None:
        """
        Send message to a flow-aware broker.

        Existing brokers can keep implementing only `kick`. New brokers may
        override this method and use `flow` to route to a concrete queue, topic,
        stream or any other transport address.

        :param message: message to send.
        :param flow: optional transport-neutral flow.
        """
        await self.kick(message)

    @abstractmethod
    def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
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
        task_name: str | None = None,
        **labels: Any,
    ) -> Callable[
        [Callable[_FuncParams, _ReturnType]],
        AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
    ]:  # pragma: no cover
        ...

    def task(  # type: ignore[misc]
        self,
        task_name: str | None = None,
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
            inner_labels: dict[str, str | int],
            inner_task_name: str | None = None,
        ) -> Callable[
            [Callable[_FuncParams, _ReturnType]],
            AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
        ]:
            def inner(
                func: Callable[_FuncParams, _ReturnType],
            ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
                return self._decorate_task(
                    func,
                    task_name=inner_task_name,
                    labels=inner_labels,
                )

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
        func: (
            Callable[_FuncParams, _ReturnType]
            | TaskDefinition[_FuncParams, _ReturnType]
        ),
        task_name: str | None = None,
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
        if isinstance(func, TaskDefinition):
            if task_name is not None or labels:
                raise ValueError(
                    "TaskDefinition already defines task_name and labels. "
                    "Register it without task_name or label overrides.",
                )
            return self.router.register_task(
                func,
                broker=self,
            )
        return self._decorate_task(
            func,
            task_name=task_name,
            labels=labels or {},
        )

    def bind_task_definition(
        self,
        task: TaskDefinition[_FuncParams, _ReturnType],
        *,
        register: bool = True,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
        """Bind an unbound task definition to this broker."""
        return self._decorate_task(
            _get_task_binding_function(task),
            task_name=task.task_name,
            labels=dict(task.labels),
            base_cls=task.base_cls,
            register=register,
            metadata_func=task.original_func,
        )

    def store_registered_task(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> None:
        """
        Store a task after router-owned registration accepted it.

        Application code should use `task` or `register_task`. This method is
        the broker/router integration boundary for cases where the router owns
        registration ordering and the broker owns task storage. Brokers with a
        custom registration store should override this method.
        """
        self._store_task(task.task_name, task)

    def _decorate_task(
        self,
        func: Callable[_FuncParams, _ReturnType],
        task_name: str | None,
        labels: dict[str, Any],
        base_cls: type[AsyncTaskiqDecoratedTask[Any, Any]] | None = None,
        *,
        register: bool = True,
        metadata_func: Callable[_FuncParams, _ReturnType] | None = None,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
        """Build and register a decorated task object."""
        metadata_source = func if metadata_func is None else metadata_func
        real_task_name = task_name
        if real_task_name is None:
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
            real_task_name = f"{fmodule}:{fname}"

        task_cls = self.decorator_class if base_cls is None else base_cls
        if not isinstance(task_cls, type) or not issubclass(
            task_cls,
            AsyncTaskiqDecoratedTask,
        ):
            raise TypeError("base_cls must be a subclass of AsyncTaskiqDecoratedTask.")

        sign = get_type_hints(metadata_source)
        return_type = None
        if "return" in sign:
            return_type = sign["return"]

        decorated_task = wraps(metadata_source)(
            task_cls(
                broker=self,
                original_func=func,
                labels=labels,
                task_name=real_task_name,
                return_type=return_type,  # type: ignore
            ),
        )

        if register:
            self._register_task(decorated_task.task_name, decorated_task)  # type: ignore
        return decorated_task  # type: ignore

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
        result_backend: "AsyncResultBackend[Any]",
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
        Method is used to register tasks.

        By default we register tasks in local task registry.
        But this behaviour can be changed in subclasses.

        This method may raise TaskBrokerMismatchError if task has already been
        registered to a different broker.

        :param task_name: Name of a task.
        :param task: Decorated task.
        """
        if task.broker != self:
            raise TaskBrokerMismatchError(broker=task.broker)
        self.router.register_task(
            task,
            broker=self,
        )
        self._store_task(task_name, task)

    def _store_task(
        self,
        task_name: str,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> None:
        """
        Store a decorated task in this broker.

        Router-managed binding uses this method after router state has accepted
        the task, so shared task registration does not re-enter router
        registration while it is already in progress.
        """
        if task.broker != self:
            raise TaskBrokerMismatchError(broker=task.broker)
        self.local_task_registry[task_name] = task

    async def __aenter__(self) -> None:
        """Starts the broker as ctx manager."""
        await self.startup()

    async def __aexit__(self, *args: object, **kwargs: Any) -> None:
        """Shuts down the broker as ctx manager."""
        await self.shutdown()
