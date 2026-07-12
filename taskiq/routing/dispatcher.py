from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from taskiq.flow import FlowProtocol
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.routing.models import TaskiqRoute
from taskiq.routing.routes import RouteRegistry
from taskiq.sending import send_task_message
from taskiq.task import AsyncTaskiqTask

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker

__all__ = ("RouterDispatcher",)

_ReturnType = TypeVar("_ReturnType")


class RouterDispatcher:
    """Send task messages through resolved router routes."""

    def __init__(self, routes: RouteRegistry) -> None:
        self._routes = routes

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
        target_route = self._routes.resolve_explicit_or_default_route(
            message.task_name,
            route=route,
            broker=broker,
            flow=flow,
        )
        target_broker = target_route.broker

        async def send(broker_message: BrokerMessage) -> None:
            await target_broker.kick_to_flow(broker_message, target_route.flow)

        return await send_task_message(
            target_broker,
            message,
            send=send,
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
        target_route = self._routes.resolve_explicit_or_default_route(
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
