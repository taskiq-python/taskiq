from __future__ import annotations

from typing import TYPE_CHECKING, Any

from taskiq.flow import FlowProtocol

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker
    from taskiq.decor import AsyncTaskiqDecoratedTask

__all__ = ("BrokerRegistry", "TaskRegistry")


class BrokerRegistry:
    """Registry of brokers attached to one router."""

    def __init__(self) -> None:
        self.brokers: dict[str, AsyncBroker] = {}
        self.default_broker: AsyncBroker | None = None

    @property
    def default_broker_name(self) -> str | None:
        """Return default broker name for compatibility and diagnostics."""
        if self.default_broker is None:
            return None
        return self.default_broker.broker_name

    def register(self, broker: AsyncBroker, name: str | None = None) -> str:
        """Register broker as a transport in this registry."""
        broker_name = name or broker.__class__.__name__
        registered = self.brokers.get(broker_name)
        if registered is not None and registered is not broker:
            raise ValueError(
                f"Broker name {broker_name!r} is already registered. "
                "Please provide an explicit unique broker_name.",
            )
        for registered_name, registered_broker in self.brokers.items():
            if registered_broker is broker and registered_name != broker_name:
                raise ValueError(
                    f"Broker is already registered as {registered_name!r}.",
                )

        self.brokers[broker_name] = broker
        if self.default_broker is None:
            self.default_broker = broker
        return broker_name

    def get(self, name: str) -> AsyncBroker:
        """Return a broker by registered name."""
        try:
            return self.brokers[name]
        except KeyError as exc:
            raise ValueError(f"Unknown broker {name!r}.") from exc

    def resolve(self, broker: AsyncBroker | None) -> AsyncBroker:
        """Resolve an explicit broker or return the default broker."""
        if isinstance(broker, str):
            raise TypeError(
                "Broker string references are not accepted here. "
                "Use router.get_broker(name) and pass the broker object.",
            )

        if broker is not None:
            for registered_broker in self.brokers.values():
                if registered_broker is broker:
                    return registered_broker
            raise ValueError("Broker is not registered in this router.")

        if self.default_broker is None:
            raise ValueError("Router doesn't have registered brokers.")
        return self.default_broker

    def default_flow(self, broker: AsyncBroker) -> FlowProtocol | None:
        """Return broker-owned default flow."""
        return getattr(broker, "default_flow", None)


class TaskRegistry:
    """Registry of tasks attached to one router."""

    def __init__(self) -> None:
        self.tasks: dict[str, AsyncTaskiqDecoratedTask[Any, Any]] = {}

    def register(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> None:
        """Register a decorated task and reject name conflicts."""
        existing_task = self.tasks.get(task.task_name)
        if existing_task is not None and existing_task is not task:
            raise ValueError(
                f"Task name {task.task_name!r} is already registered "
                "in this router.",
            )
        self.tasks[task.task_name] = task

    def find(
        self,
        task_name: str,
    ) -> AsyncTaskiqDecoratedTask[Any, Any] | None:
        """Find a task by name."""
        return self.tasks.get(task_name)

    def get_all(self) -> dict[str, AsyncTaskiqDecoratedTask[Any, Any]]:
        """Return all tasks registered in this registry."""
        return dict(self.tasks)
