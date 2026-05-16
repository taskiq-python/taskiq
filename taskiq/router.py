from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast, overload

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.flow import Flow
from taskiq.message import TaskiqMessage
from taskiq.task import AsyncTaskiqTask
from taskiq.task_builder import TaskDefinition
from taskiq.utils import maybe_awaitable

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker
    from taskiq.decor import AsyncTaskiqDecoratedTask

__all__ = ("TaskiqRoute", "TaskiqRouter")

_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")

logger = getLogger("taskiq.router")


@dataclass(frozen=True, slots=True)
class TaskiqRoute:
    """Resolved outbound route for a task invocation."""

    broker_name: str
    flow: Flow | None = None


class TaskiqRouter:
    """Registry and routing layer shared by one or more brokers."""

    def __init__(self) -> None:
        self.brokers: dict[str, AsyncBroker] = {}
        self.default_broker_name: str | None = None
        self.task_registry: dict[str, AsyncTaskiqDecoratedTask[Any, Any]] = {}
        self.routes: dict[str, TaskiqRoute] = {}

    def set_broker(
        self,
        broker: AsyncBroker,
        name: str | None = None,
        default_flow: Flow | None = None,
    ) -> str:
        """Register broker as a transport in this router."""
        broker_name = name or broker.__class__.__name__
        registered = self.brokers.get(broker_name)
        if registered is not None and registered is not broker:
            raise ValueError(
                f"Broker name {broker_name!r} is already registered. "
                "Please provide an explicit unique broker_name.",
            )
        self.brokers[broker_name] = broker
        if self.default_broker_name is None:
            self.default_broker_name = broker_name
        return broker_name

    def find_task(
        self,
        task_name: str,
    ) -> AsyncTaskiqDecoratedTask[Any, Any] | None:
        """Find a task by name."""
        return self.task_registry.get(task_name)

    def get_all_tasks(self) -> dict[str, AsyncTaskiqDecoratedTask[Any, Any]]:
        """Return all tasks registered in this router."""
        return dict(self.task_registry)

    def register_task(
        self,
        task: (
            AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]
            | TaskDefinition[_FuncParams, _ReturnType]
        ),
        broker: AsyncBroker | str | None = None,
        flow: Flow | None = None,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
        """Register a bound task or bind a task definition to a broker."""
        if isinstance(task, TaskDefinition):
            target_broker = self._resolve_broker(broker)
            registered_task = target_broker.register_task(
                task.original_func,
                task_name=task.task_name,
                **task.labels,
            )
            if flow is not None:
                self.route_task(task.task_name, broker=target_broker, flow=flow)
            return registered_task

        self.task_registry[task.task_name] = task
        route_broker: AsyncBroker | str | None = broker
        if route_broker is None:
            route_broker = getattr(task, "broker", None)
        if route_broker is not None or flow is not None:
            self.route_task(task.task_name, broker=route_broker, flow=flow)
        return task

    @overload
    def task(
        self,
        task_name: Callable[_FuncParams, _ReturnType],
        *,
        broker: AsyncBroker | str | None = None,
        flow: Flow | None = None,
        **labels: Any,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]: ...

    @overload
    def task(
        self,
        task_name: str | None = None,
        *,
        broker: AsyncBroker | str | None = None,
        flow: Flow | None = None,
        **labels: Any,
    ) -> Callable[
        [Callable[_FuncParams, _ReturnType]],
        AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
    ]: ...

    def task(
        self,
        task_name: str | Callable[_FuncParams, _ReturnType] | None = None,
        *,
        broker: AsyncBroker | str | None = None,
        flow: Flow | None = None,
        **labels: Any,
    ) -> Any:
        """Decorate and register a task through this router."""

        def register(
            func: Callable[_FuncParams, _ReturnType],
        ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
            target_broker = self._resolve_broker(broker)
            real_task_name = task_name if not callable(task_name) else None
            task = target_broker.task(task_name=real_task_name, **labels)(func)
            if flow is not None:
                self.route_task(task.task_name, broker=target_broker, flow=flow)
            return task

        if callable(task_name):
            function = task_name
            task_name = None
            return register(function)

        return register

    def route_task(
        self,
        task_name: str,
        broker: AsyncBroker | str | None = None,
        flow: Flow | None = None,
    ) -> TaskiqRoute:
        """Set default outbound route for a task."""
        broker_name = self._resolve_broker_name(broker)
        route = TaskiqRoute(broker_name=broker_name, flow=flow)
        self.routes[task_name] = route
        return route

    def resolve_route(
        self,
        task_name: str,
        broker: AsyncBroker | str | None = None,
        flow: Flow | None = None,
    ) -> TaskiqRoute:
        """Resolve outbound route for a task invocation."""
        if broker is not None:
            broker_name = self._resolve_broker_name(broker)
            route_flow = flow
            if route_flow is None:
                route_flow = self._broker_default_flow(broker_name)
            return TaskiqRoute(
                broker_name=broker_name,
                flow=route_flow,
            )

        route = self.routes.get(task_name)
        if route is not None:
            if flow is None:
                return route
            return TaskiqRoute(broker_name=route.broker_name, flow=flow)

        broker_name = self._resolve_broker_name(None)
        route_flow = flow
        if route_flow is None:
            route_flow = self._broker_default_flow(broker_name)
        return TaskiqRoute(
            broker_name=broker_name,
            flow=route_flow,
        )

    async def kiq(
        self,
        message: TaskiqMessage,
        *,
        broker: AsyncBroker | str | None = None,
        flow: Flow | None = None,
        return_type: type[_ReturnType] | None = None,
    ) -> AsyncTaskiqTask[_ReturnType]:
        """Send message through the resolved broker and flow."""
        route = self.resolve_route(message.task_name, broker=broker, flow=flow)
        target_broker = self.brokers[route.broker_name]

        for middleware in target_broker.middlewares:
            if middleware.__class__.pre_send != TaskiqMiddleware.pre_send:
                message = await maybe_awaitable(middleware.pre_send(message))
        broker_message = target_broker.formatter.dumps(message)
        await target_broker.kick_to_flow(broker_message, route.flow)

        for middleware in reversed(target_broker.middlewares):
            if middleware.__class__.post_send != TaskiqMiddleware.post_send:
                await maybe_awaitable(middleware.post_send(message))

        return AsyncTaskiqTask(
            task_id=message.task_id,
            result_backend=cast(
                AsyncResultBackend[_ReturnType],
                target_broker.result_backend,
            ),
            return_type=return_type,
        )

    async def requeue(
        self,
        message: TaskiqMessage,
        *,
        broker: AsyncBroker | str | None = None,
        flow: Flow | None = None,
    ) -> None:
        """Send an existing message again through the resolved route."""
        route = self.resolve_route(message.task_name, broker=broker, flow=flow)
        target_broker = self.brokers[route.broker_name]
        await target_broker.kick_to_flow(
            target_broker.formatter.dumps(message),
            route.flow,
        )

    def _resolve_broker(self, broker: AsyncBroker | str | None) -> AsyncBroker:
        broker_name = self._resolve_broker_name(broker)
        return self.brokers[broker_name]

    def _resolve_broker_name(self, broker: AsyncBroker | str | None) -> str:
        if isinstance(broker, str):
            if broker not in self.brokers:
                raise ValueError(f"Unknown broker {broker!r}.")
            return broker

        if broker is not None:
            broker_name = getattr(broker, "broker_name", None)
            if broker_name is not None and broker_name in self.brokers:
                return broker_name
            for registered_name, registered_broker in self.brokers.items():
                if registered_broker is broker:
                    return registered_name
            raise ValueError("Broker is not registered in this router.")

        if self.default_broker_name is None:
            raise ValueError("Router doesn't have registered brokers.")
        return self.default_broker_name

    def _broker_default_flow(self, broker_name: str) -> Flow | None:
        return getattr(self.brokers[broker_name], "default_flow", None)
