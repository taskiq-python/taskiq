from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Protocol, runtime_checkable

__all__ = ("Flow", "FlowIdentity", "FlowProtocol", "get_flow_identity")


@dataclass(frozen=True, slots=True)
class FlowIdentity:
    """Stable identity for one logical flow in routing and listen plans."""

    name: str


@runtime_checkable
class FlowProtocol(Protocol):
    """Transport-neutral flow contract accepted by routers and brokers."""

    @property
    def name(self) -> str:
        """Return transport-neutral flow name."""
        ...

    def broker_options(self) -> Mapping[str, object]:
        """Return options relevant for a concrete broker implementation."""
        ...


@dataclass(frozen=True, slots=True)
class Flow:
    """Generic transport-neutral flow address.

    Broker packages can provide their own flow objects that implement
    FlowProtocol, for example RabbitQueue, KafkaTopic, NatsSubject or RedisQueue.
    This generic value object is intentionally small and works as a common
    fallback for brokers that only need a named address with optional metadata.
    """

    name: str
    options: Mapping[str, object] = field(
        default_factory=dict,
        compare=False,
        hash=False,
    )

    def __post_init__(self) -> None:
        """Freeze options to keep Flow a stable value object."""
        object.__setattr__(self, "options", MappingProxyType(dict(self.options)))

    def with_options(self, **options: object) -> "Flow":
        """Return the same flow with additional generic options."""
        return replace(self, options={**self.options, **options})

    @property
    def identity(self) -> FlowIdentity:
        """Return routing and subscription identity for this flow."""
        return get_flow_identity(self)

    def broker_options(self) -> Mapping[str, object]:
        """Return transport options for broker-specific implementations."""
        return dict(self.options)


def get_flow_identity(flow: FlowProtocol) -> FlowIdentity:
    """
    Return stable identity for a flow.

    Flow identity is intentionally transport-neutral and based on the logical
    flow name. Broker-specific declaration options are validated separately by
    the subscription plan.
    """
    return FlowIdentity(name=flow.name)
