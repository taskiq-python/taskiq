from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace
from typing import TYPE_CHECKING

from taskiq.flow import FlowProtocol, get_flow_identity
from taskiq.routing.models import TaskiqSubscription
from taskiq.routing.registries import BrokerRegistry

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker

__all__ = ("SubscriptionPlan",)


class SubscriptionPlan:
    """
    Inbound listen-plan subscriptions for flow-aware brokers.

    This is mutable setup-time configuration, not a thread-safe runtime
    coordination primitive.
    """

    def __init__(self, brokers: BrokerRegistry) -> None:
        self._brokers = brokers
        self._subscriptions: list[TaskiqSubscription] = []

    def subscribe(
        self,
        broker: AsyncBroker,
        flow: FlowProtocol,
        task_names: Iterable[str],
    ) -> TaskiqSubscription:
        """Register an inbound flow subscription for a broker."""
        target_broker = self._brokers.resolve(broker)
        resolved_task_names = frozenset(task_names)
        default_flow = self._brokers.default_flow(target_broker)
        if default_flow is not None:
            self._ensure_compatible_flow(
                broker=target_broker,
                registered_flow=default_flow,
                new_flow=flow,
            )

        for subscription_index, subscription in enumerate(self._subscriptions):
            if subscription.broker is target_broker and self._is_same_flow(
                subscription.flow,
                flow,
            ):
                self._ensure_compatible_flow(
                    broker=target_broker,
                    registered_flow=subscription.flow,
                    new_flow=flow,
                )
                updated = replace(
                    subscription,
                    task_names=subscription.task_names | resolved_task_names,
                )
                self._subscriptions[subscription_index] = updated
                return updated

        subscription = TaskiqSubscription(
            broker=target_broker,
            flow=flow,
            task_names=resolved_task_names,
        )
        self._subscriptions.append(subscription)
        return subscription

    def unsubscribe(
        self,
        broker: AsyncBroker,
        flow: FlowProtocol,
    ) -> TaskiqSubscription | None:
        """Remove and return one compatible broker/flow subscription."""
        target_broker = self._brokers.resolve(broker)
        for index, subscription in enumerate(self._subscriptions):
            if subscription.broker is target_broker and self._is_same_flow(
                subscription.flow,
                flow,
            ):
                self._ensure_compatible_flow(
                    broker=target_broker,
                    registered_flow=subscription.flow,
                    new_flow=flow,
                )
                return self._subscriptions.pop(index)
        return None

    def get(
        self,
        broker: AsyncBroker | None = None,
    ) -> tuple[TaskiqSubscription, ...]:
        """Return registered inbound subscriptions."""
        if broker is None:
            return tuple(self._subscriptions)
        target_broker = self._brokers.resolve(broker)
        return tuple(
            subscription
            for subscription in self._subscriptions
            if subscription.broker is target_broker
        )

    def get_broker_flows(self, broker: AsyncBroker) -> tuple[FlowProtocol, ...]:
        """Return flows a broker should subscribe to."""
        target_broker = self._brokers.resolve(broker)
        flows = [subscription.flow for subscription in self.get(target_broker)]
        default_flow = self._brokers.default_flow(target_broker)
        if default_flow is not None and not self._contains_flow_identity(
            flows,
            default_flow,
        ):
            flows.insert(0, default_flow)
        elif default_flow is not None:
            for flow in flows:
                if self._is_same_flow(flow, default_flow):
                    self._ensure_compatible_flow(
                        broker=target_broker,
                        registered_flow=flow,
                        new_flow=default_flow,
                    )
                    break
        return tuple(flows)

    def _contains_flow_identity(
        self,
        flows: Iterable[FlowProtocol],
        flow: FlowProtocol,
    ) -> bool:
        return any(
            self._is_same_flow(registered_flow, flow) for registered_flow in flows
        )

    def _is_same_flow(
        self,
        first_flow: FlowProtocol,
        second_flow: FlowProtocol,
    ) -> bool:
        return get_flow_identity(first_flow) == get_flow_identity(second_flow)

    def _ensure_compatible_flow(
        self,
        *,
        broker: AsyncBroker,
        registered_flow: FlowProtocol,
        new_flow: FlowProtocol,
    ) -> None:
        if not self._is_same_flow(registered_flow, new_flow):
            return

        registered_options = dict(registered_flow.broker_options())
        new_options = dict(new_flow.broker_options())
        if registered_options == new_options:
            return

        flow_identity = get_flow_identity(new_flow)
        raise ValueError(
            f"Flow {flow_identity.name!r} is already registered for broker "
            f"{broker.broker_name!r} with different broker options.",
        )
