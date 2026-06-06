from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar, cast

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.flow import FlowProtocol
from taskiq.message import TaskiqMessage
from taskiq.routing.models import TaskiqRoute
from taskiq.routing.routes import RouteRegistry
from taskiq.task import AsyncTaskiqTask
from taskiq.utils import maybe_awaitable

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker

__all__ = ("RouterDispatcher",)

_ReturnType = TypeVar("_ReturnType")


class RouterDispatcher:
    """Send task messages through resolved router routes."""

    def __init__(self, routes: RouteRegistry) -> None:
        self.routes = routes

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
        target_route = self.routes.resolve_explicit_or_default_route(
            message.task_name,
            route=route,
            broker=broker,
            flow=flow,
        )
        target_broker = target_route.broker

        for middleware in target_broker.middlewares:
            if middleware.__class__.pre_send != TaskiqMiddleware.pre_send:
                message = await maybe_awaitable(middleware.pre_send(message))
        broker_message = target_broker.formatter.dumps(message)
        await target_broker.kick_to_flow(broker_message, target_route.flow)

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
        route: TaskiqRoute | None = None,
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> None:
        """Send an existing message again through the resolved route."""
        target_route = self.routes.resolve_explicit_or_default_route(
            message.task_name,
            route=route,
            broker=broker,
            flow=flow,
        )
        target_broker = target_route.broker
        await target_broker.kick_to_flow(
            target_broker.formatter.dumps(message),
            target_route.flow,
        )
