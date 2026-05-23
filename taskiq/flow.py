from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Protocol, runtime_checkable

__all__ = ("Flow", "FlowProtocol")


@runtime_checkable
class FlowProtocol(Protocol):
    """Transport-neutral flow contract accepted by routers and brokers."""

    name: str

    def broker_options(self, broker_name: str) -> Mapping[str, object]:
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

    def with_options(self, **options: object) -> "Flow":
        """Return the same flow with additional generic options."""
        return replace(self, options={**self.options, **options})

    def broker_options(self, broker_name: str) -> Mapping[str, object]:
        """Return transport options for broker-specific implementations."""
        return dict(self.options)
