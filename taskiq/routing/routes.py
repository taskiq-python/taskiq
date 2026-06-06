from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from taskiq.flow import FlowProtocol
from taskiq.routing.models import TaskiqRoute
from taskiq.routing.references import resolve_task_name
from taskiq.routing.registries import BrokerRegistry

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker
    from taskiq.decor import AsyncTaskiqDecoratedTask

__all__ = ("RouteRegistry",)


class RouteRegistry:
    """Outbound route policy for task invocations."""

    def __init__(self, brokers: BrokerRegistry) -> None:
        self.brokers = brokers
        self.routes: dict[str, TaskiqRoute] = {}

    def build_route(
        self,
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> TaskiqRoute:
        """Build a route without mutating route state."""
        target_broker = self.brokers.resolve(broker)
        route_flow = flow
        if route_flow is None:
            route_flow = self.brokers.default_flow(target_broker)
        return TaskiqRoute(broker=target_broker, flow=route_flow)

    def set_route(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> TaskiqRoute:
        """Set default outbound route for a task."""
        task_name = resolve_task_name(task)
        route = self.build_route(broker=broker, flow=flow)
        self.routes[task_name] = route
        return route

    def set_resolved_route(self, task_name: str, route: TaskiqRoute) -> TaskiqRoute:
        """Store an already resolved route for a task."""
        self.routes[task_name] = route
        return route

    def resolve_route(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> TaskiqRoute:
        """Resolve outbound route for a task invocation."""
        task_name = resolve_task_name(task)
        if broker is not None:
            target_broker = self.brokers.resolve(broker)
            route_flow = self._resolve_flow_for_broker_override(
                task_name,
                target_broker,
                flow,
            )
            return TaskiqRoute(broker=target_broker, flow=route_flow)

        route = self.routes.get(task_name)
        if route is not None:
            if flow is None:
                return route
            return replace(route, flow=flow)

        return self.build_route(flow=flow)

    def resolve_explicit_or_default_route(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
        *,
        route: TaskiqRoute | None,
        broker: AsyncBroker | None,
        flow: FlowProtocol | None,
    ) -> TaskiqRoute:
        """Resolve a route from explicit route or broker/flow overrides."""
        if route is not None:
            if broker is not None or flow is not None:
                raise ValueError("Pass either route or broker/flow overrides.")
            self.brokers.resolve(route.broker)
            return route
        return self.resolve_route(task, broker=broker, flow=flow)

    def _resolve_flow_for_broker_override(
        self,
        task_name: str,
        broker: AsyncBroker,
        flow: FlowProtocol | None,
    ) -> FlowProtocol | None:
        if flow is not None:
            return flow
        registered_route = self.routes.get(task_name)
        if registered_route is not None and registered_route.broker is broker:
            return registered_route.flow
        return self.brokers.default_flow(broker)
