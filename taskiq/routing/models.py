from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from taskiq.flow import FlowProtocol

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker

__all__ = ("TaskiqRoute", "TaskiqSubscription")


@dataclass(frozen=True, slots=True)
class TaskiqRoute:
    """Resolved outbound route for a task invocation."""

    broker: AsyncBroker
    flow: FlowProtocol | None = None

    @property
    def broker_name(self) -> str:
        """Return registered broker name for diagnostics."""
        return self.broker.broker_name


@dataclass(frozen=True, slots=True)
class TaskiqSubscription:
    """
    Inbound flow subscription owned by a router.

    `task_names` is diagnostic listen-plan metadata. It records which task
    declarations caused a broker to listen to this flow; worker execution still
    resolves inbound messages by `TaskiqMessage.task_name`.
    """

    broker: AsyncBroker
    flow: FlowProtocol
    task_names: frozenset[str] = field(default_factory=frozenset)

    @property
    def broker_name(self) -> str:
        """Return registered broker name for diagnostics."""
        return self.broker.broker_name
