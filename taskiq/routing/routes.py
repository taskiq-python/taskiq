from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from taskiq.flow import FlowProtocol
from taskiq.routing.models import TaskiqRoute
from taskiq.routing.references import resolve_task_name
from taskiq.routing.registries import BrokerRegistry

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker
    from taskiq.decor import AsyncTaskiqDecoratedTask

__all__ = ("RouteRegistry",)


@dataclass(frozen=True, slots=True)
class _TaskRoutePolicy:
    """Stored outbound policy before broker default-flow resolution."""

    broker: AsyncBroker
    flow: FlowProtocol | None


class RouteRegistry:
    """Outbound route policy for task invocations."""

    def __init__(self, brokers: BrokerRegistry) -> None:
        self._brokers = brokers
        self._routes: dict[str, _TaskRoutePolicy] = {}

    def get_all(self) -> dict[str, TaskiqRoute]:
        """Return resolved route snapshots keyed by task name."""
        return {
            task_name: self._resolve_policy(policy)
            for task_name, policy in self._routes.items()
        }

    def has_route(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
    ) -> bool:
        """Return whether a task has an explicit route policy."""
        return resolve_task_name(task) in self._routes

    def remove_route(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
    ) -> TaskiqRoute | None:
        """Remove and return a task's resolved route policy, if present."""
        policy = self._routes.pop(resolve_task_name(task), None)
        if policy is None:
            return None
        return self._resolve_policy(policy)

    def build_route(
        self,
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> TaskiqRoute:
        """Build a route without mutating route state."""
        target_broker = self._brokers.resolve(broker)
        route_flow = flow
        if route_flow is None:
            route_flow = self._brokers.default_flow(target_broker)
        return TaskiqRoute(broker=target_broker, flow=route_flow)

    def set_route(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> TaskiqRoute:
        """
        Set default outbound route for a task.

        A missing flow is stored as a dynamic broker-default policy. The
        returned route is resolved against the broker's current default flow.
        """
        task_name = resolve_task_name(task)
        target_broker = self._brokers.resolve(broker)
        policy = _TaskRoutePolicy(broker=target_broker, flow=flow)
        self._routes[task_name] = policy
        return self._resolve_policy(policy)

    def resolve_route(
        self,
        task: str | AsyncTaskiqDecoratedTask[Any, Any],
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> TaskiqRoute:
        """Resolve outbound route for a task invocation."""
        task_name = resolve_task_name(task)
        if broker is not None:
            target_broker = self._brokers.resolve(broker)
            route_flow = self._resolve_flow_for_broker_override(
                task_name,
                target_broker,
                flow,
            )
            return TaskiqRoute(broker=target_broker, flow=route_flow)

        policy = self._routes.get(task_name)
        if policy is not None:
            if flow is not None:
                return TaskiqRoute(broker=policy.broker, flow=flow)
            return self._resolve_policy(policy)

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
            self._brokers.resolve(route.broker)
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
        registered_policy = self._routes.get(task_name)
        if (
            registered_policy is not None
            and registered_policy.broker is broker
            and registered_policy.flow is not None
        ):
            return registered_policy.flow
        return self._brokers.default_flow(broker)

    def _resolve_policy(self, policy: _TaskRoutePolicy) -> TaskiqRoute:
        flow = policy.flow
        if flow is None:
            flow = self._brokers.default_flow(policy.broker)
        return TaskiqRoute(broker=policy.broker, flow=flow)
