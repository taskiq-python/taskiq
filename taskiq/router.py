from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast, overload

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.flow import FlowProtocol
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

    broker: AsyncBroker
    flow: FlowProtocol | None = None

    @property
    def broker_name(self) -> str:
        """Return registered broker name for compatibility and diagnostics."""
        return self.broker.broker_name


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
        default_flow: FlowProtocol | None = None,
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
        flow: FlowProtocol | None = None,
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
        flow: FlowProtocol | None = None,
        **labels: Any,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]: ...

    @overload
    def task(
        self,
        task_name: str | None = None,
        *,
        broker: AsyncBroker | str | None = None,
        flow: FlowProtocol | None = None,
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
        flow: FlowProtocol | None = None,
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
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
        broker: AsyncBroker | str | None = None,
        flow: FlowProtocol | None = None,
    ) -> TaskiqRoute:
        """Set default outbound route for a task."""
        task_name = self._resolve_task_name(task)
        target_broker = self._resolve_broker(broker)
        route = TaskiqRoute(broker=target_broker, flow=flow)
        self.routes[task_name] = route
        return route

    def resolve_route(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
        broker: AsyncBroker | str | None = None,
        flow: FlowProtocol | None = None,
    ) -> TaskiqRoute:
        """Resolve outbound route for a task invocation."""
        task_name = self._resolve_task_name(task)
        if broker is not None:
            target_broker = self._resolve_broker(broker)
            route_flow = flow
            if route_flow is None:
                route_flow = self._broker_default_flow(target_broker)
            return TaskiqRoute(
                broker=target_broker,
                flow=route_flow,
            )

        route = self.routes.get(task_name)
        if route is not None:
            if flow is None:
                return route
            return TaskiqRoute(broker=route.broker, flow=flow)

        target_broker = self._resolve_broker(None)
        route_flow = flow
        if route_flow is None:
            route_flow = self._broker_default_flow(target_broker)
        return TaskiqRoute(
            broker=target_broker,
            flow=route_flow,
        )

    async def kiq(
        self,
        message: TaskiqMessage,
        *,
        broker: AsyncBroker | str | None = None,
        flow: FlowProtocol | None = None,
        return_type: type[_ReturnType] | None = None,
    ) -> AsyncTaskiqTask[_ReturnType]:
        """Send message through the resolved broker and flow."""
        route = self.resolve_route(message.task_name, broker=broker, flow=flow)
        target_broker = route.broker

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
        flow: FlowProtocol | None = None,
    ) -> None:
        """Send an existing message again through the resolved route."""
        route = self.resolve_route(message.task_name, broker=broker, flow=flow)
        target_broker = route.broker
        await target_broker.kick_to_flow(
            target_broker.formatter.dumps(message),
            route.flow,
        )

    def _resolve_broker(self, broker: AsyncBroker | str | None) -> AsyncBroker:
        if isinstance(broker, str):
            if broker not in self.brokers:
                raise ValueError(f"Unknown broker {broker!r}.")
            return self.brokers[broker]

        if broker is not None:
            broker_name = getattr(broker, "broker_name", None)
            if isinstance(broker_name, str):
                registered_broker = self.brokers.get(broker_name)
                if registered_broker is broker:
                    return broker
            for registered_broker in self.brokers.values():
                if registered_broker is broker:
                    return registered_broker
            raise ValueError("Broker is not registered in this router.")

        if self.default_broker_name is None:
            raise ValueError("Router doesn't have registered brokers.")
        return self.brokers[self.default_broker_name]

    def _resolve_broker_name(self, broker: AsyncBroker | str | None) -> str:
        return self._resolve_broker(broker).broker_name

    def _resolve_task_name(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
    ) -> str:
        if isinstance(task, str):
            return task
        task_name = getattr(task, "task_name", None)
        if isinstance(task_name, str):
            return task_name
        raise TypeError("Route task must be a task name or decorated task.")

    def _broker_default_flow(self, broker: AsyncBroker) -> FlowProtocol | None:
        return getattr(broker, "default_flow", None)
