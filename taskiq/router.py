from __future__ import annotations

import warnings
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, overload

from taskiq.flow import FlowProtocol
from taskiq.message import TaskiqMessage
from taskiq.routing import (
    BrokerRegistry,
    RouterDispatcher,
    RouteRegistry,
    SubscriptionPlan,
    TaskiqRoute,
    TaskiqSubscription,
    TaskRegistry,
)
from taskiq.routing.references import resolve_task_name
from taskiq.task import AsyncTaskiqTask
from taskiq.task_builder import TaskDefinition
from taskiq.warnings import TaskiqDeprecationWarning

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker
    from taskiq.decor import AsyncTaskiqDecoratedTask

__all__ = ("TaskiqRoute", "TaskiqRouter", "TaskiqSubscription")

TaskiqRoute.__module__ = __name__
TaskiqSubscription.__module__ = __name__

_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")


class TaskiqRouter:
    """Facade for task registry, routing policy, subscriptions and dispatch."""

    def __init__(self) -> None:
        self._brokers = BrokerRegistry()
        self._tasks = TaskRegistry()
        self._routes = RouteRegistry(self._brokers)
        self._subscriptions = SubscriptionPlan(self._brokers)
        self._dispatcher = RouterDispatcher(self._routes)

    @property
    def brokers(self) -> Mapping[str, AsyncBroker]:
        """Return an immutable snapshot of registered brokers."""
        return MappingProxyType(self._brokers.get_all())

    @property
    def default_broker(self) -> AsyncBroker | None:
        """Return default broker for compatibility."""
        return self._brokers.default_broker

    @default_broker.setter
    def default_broker(self, broker: AsyncBroker | None) -> None:
        self._brokers.set_default(broker)

    @property
    def task_registry(
        self,
    ) -> Mapping[str, AsyncTaskiqDecoratedTask[Any, Any]]:
        """Return an immutable snapshot of registered tasks."""
        return MappingProxyType(self._tasks.get_all())

    @property
    def routes(self) -> Mapping[str, TaskiqRoute]:
        """Return an immutable snapshot of resolved task routes."""
        return MappingProxyType(self._routes.get_all())

    @property
    def subscriptions(self) -> tuple[TaskiqSubscription, ...]:
        """Return an immutable snapshot of inbound subscriptions."""
        return self._subscriptions.get()

    @property
    def default_broker_name(self) -> str | None:
        """Return default broker name for compatibility and diagnostics."""
        return self._brokers.default_broker_name

    def set_broker(
        self,
        broker: AsyncBroker,
        name: str | None = None,
        default_flow: FlowProtocol | None = None,
    ) -> str:
        """Register broker as a transport in this router."""
        if getattr(broker, "router", self) is not self:
            raise ValueError(
                "Broker is attached to another router. "
                "Pass router=... when creating the broker.",
            )
        broker_name = self._brokers.register(broker, name=name)
        if default_flow is not None:
            broker.default_flow = default_flow
        return broker_name

    def get_broker(self, name: str) -> AsyncBroker:
        """Return a broker by registered name."""
        return self._brokers.get(name)

    def find_task(
        self,
        task_name: str,
    ) -> AsyncTaskiqDecoratedTask[Any, Any] | None:
        """Find a task by name."""
        return self._tasks.find(task_name)

    def get_all_tasks(self) -> dict[str, AsyncTaskiqDecoratedTask[Any, Any]]:
        """Return all tasks registered in this router."""
        return self._tasks.get_all()

    def register_task(
        self,
        task: (
            AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]
            | TaskDefinition[_FuncParams, _ReturnType]
        ),
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
        """Register a bound task or bind a task definition to a broker."""
        if isinstance(task, TaskDefinition):
            target_broker = self._brokers.resolve(broker)
            self._tasks.ensure_name_available(task.task_name)
            registered_task = target_broker.bind_task_definition(
                task,
                register=False,
            )
            target_broker.store_registered_task(registered_task)
            return self._register_bound_task(
                registered_task,
                broker=target_broker,
                flow=flow,
            )

        return self._register_bound_task(task, broker=broker, flow=flow)

    @overload
    def task(
        self,
        task_name: Callable[_FuncParams, _ReturnType],
        *,
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
        **labels: Any,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]: ...

    @overload
    def task(
        self,
        task_name: str | None = None,
        *,
        broker: AsyncBroker | None = None,
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
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
        **labels: Any,
    ) -> Any:
        """Decorate and register a task through this router."""

        def register(
            func: Callable[_FuncParams, _ReturnType],
        ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
            target_broker = self._brokers.resolve(broker)
            real_task_name: str | None = None if callable(task_name) else task_name
            task = target_broker.task(task_name=real_task_name, **labels)(func)
            if flow is not None:
                self.route_task(task, broker=target_broker, flow=flow)
            return task

        if callable(task_name):
            function = task_name
            return register(function)

        return register

    def route_task(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
        *,
        subscribe: bool = False,
    ) -> TaskiqRoute:
        """Set default outbound route for a task."""
        task_name = resolve_task_name(task)
        if not subscribe:
            return self._routes.set_route(task_name, broker=broker, flow=flow)

        route = self._routes.build_route(broker=broker, flow=flow)
        warnings.warn(
            "`route_task(..., subscribe=True)` is deprecated. "
            "Use `router.subscribe(...)` to add inbound flow subscriptions.",
            TaskiqDeprecationWarning,
            stacklevel=2,
        )
        if route.flow is not None:
            self.subscribe(route.broker, route.flow, task_name)
        return self._routes.set_route(task_name, broker=broker, flow=flow)

    def resolve_route(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> TaskiqRoute:
        """Resolve outbound route for a task invocation."""
        return self._routes.resolve_route(task, broker=broker, flow=flow)

    def has_route(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
    ) -> bool:
        """Return whether a task has an explicit outbound route policy."""
        return self._routes.has_route(task)

    def remove_route(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
    ) -> TaskiqRoute | None:
        """Remove and return a task's resolved outbound route, if present."""
        return self._routes.remove_route(task)

    def subscribe(
        self,
        broker: AsyncBroker,
        flow: FlowProtocol,
        *tasks: str | AsyncTaskiqDecoratedTask[Any, Any],
    ) -> TaskiqSubscription:
        """Register an inbound flow subscription for a broker."""
        task_names = tuple(resolve_task_name(task) for task in tasks)
        return self._subscriptions.subscribe(
            broker,
            flow,
            task_names,
        )

    def unsubscribe(
        self,
        broker: AsyncBroker,
        flow: FlowProtocol,
    ) -> TaskiqSubscription | None:
        """Remove and return one inbound broker/flow subscription."""
        return self._subscriptions.unsubscribe(broker, flow)

    def get_subscriptions(
        self,
        broker: AsyncBroker | None = None,
    ) -> tuple[TaskiqSubscription, ...]:
        """Return registered inbound subscriptions."""
        return self._subscriptions.get(broker)

    def get_broker_flows(self, broker: AsyncBroker) -> tuple[FlowProtocol, ...]:
        """Return flows a broker should subscribe to."""
        return self._subscriptions.get_broker_flows(broker)

    async def kiq(
        self,
        message: TaskiqMessage,
        *,
        route: TaskiqRoute | None = None,
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
        return_type: type[_ReturnType] | None = None,
    ) -> AsyncTaskiqTask[_ReturnType]:
        """Send message through the resolved broker and flow."""
        return await self._dispatcher.kiq(
            message,
            route=route,
            broker=broker,
            flow=flow,
            return_type=return_type,
        )

    async def requeue(
        self,
        message: TaskiqMessage,
        *,
        route: TaskiqRoute | None = None,
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> None:
        """Send an existing message again through the resolved route."""
        await self._dispatcher.requeue(
            message,
            route=route,
            broker=broker,
            flow=flow,
        )

    def _register_bound_task(
        self,
        task: AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType],
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> AsyncTaskiqDecoratedTask[_FuncParams, _ReturnType]:
        route_broker = broker
        if route_broker is None:
            route_broker = getattr(task, "broker", None)

        self._tasks.ensure_available(task)
        if route_broker is not None or flow is not None:
            self._routes.build_route(broker=route_broker, flow=flow)

        self._tasks.register(task)
        if route_broker is not None or flow is not None:
            self._routes.set_route(
                task.task_name,
                broker=route_broker,
                flow=flow,
            )
        return task
