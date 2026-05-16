import enum
from dataclasses import dataclass, field, replace
from typing import Any

__all__ = ("Flow", "FlowKind")


@enum.unique
class FlowKind(str, enum.Enum):
    """Transport-neutral flow shape."""

    QUEUE = "queue"
    TOPIC = "topic"
    STREAM = "stream"


@dataclass(frozen=True, slots=True)
class Flow:
    """Transport-neutral publish or subscribe address.

    Plain flows are intentionally generic. Every broker may interpret a flow
    using its own defaults: queue name, topic, stream, channel, list key, or any
    other transport address.

    Broker packages can subclass this value object to expose transport-specific
    details while still accepting plain Flow instances.
    """

    name: str
    kind: FlowKind = FlowKind.QUEUE
    options: dict[str, Any] = field(
        default_factory=dict,
        compare=False,
        hash=False,
    )

    @classmethod
    def queue(cls, name: str, **options: Any) -> "Flow":
        """Create a queue-like flow."""
        return cls(name=name, kind=FlowKind.QUEUE, options=options)

    @classmethod
    def topic(cls, name: str, **options: Any) -> "Flow":
        """Create a topic-like flow."""
        return cls(name=name, kind=FlowKind.TOPIC, options=options)

    @classmethod
    def stream(cls, name: str, **options: Any) -> "Flow":
        """Create a stream-like flow."""
        return cls(name=name, kind=FlowKind.STREAM, options=options)

    def with_options(self, **options: Any) -> "Flow":
        """Return the same flow with additional generic options."""
        return replace(self, options={**self.options, **options})

    def broker_options(self, broker_name: str) -> dict[str, Any]:
        """Return transport options for broker-specific implementations."""
        return dict(self.options)
