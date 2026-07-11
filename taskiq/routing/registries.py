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
        self._brokers: dict[str, AsyncBroker] = {}
        self._default_broker: AsyncBroker | None = None

    @property
    def default_broker(self) -> AsyncBroker | None:
        """Return the configured default broker."""
        return self._default_broker

    @property
    def default_broker_name(self) -> str | None:
        """Return default broker name for compatibility and diagnostics."""
        if self._default_broker is None:
            return None
        return self._default_broker.broker_name

    def get_all(self) -> dict[str, AsyncBroker]:
        """Return a snapshot of registered brokers."""
        return dict(self._brokers)

    def register(self, broker: AsyncBroker, name: str | None = None) -> str:
        """Register broker as a transport in this registry."""
        broker_name = name or broker.__class__.__name__
        registered = self._brokers.get(broker_name)
        if registered is not None and registered is not broker:
            raise ValueError(
                f"Broker name {broker_name!r} is already registered. "
                "Please provide an explicit unique broker_name.",
            )
        for registered_name, registered_broker in self._brokers.items():
            if registered_broker is broker and registered_name != broker_name:
                raise ValueError(
                    f"Broker is already registered as {registered_name!r}.",
                )

        self._brokers[broker_name] = broker
        if self._default_broker is None:
            self._default_broker = broker
        return broker_name

    def get(self, name: str) -> AsyncBroker:
        """Return a broker by registered name."""
        try:
            return self._brokers[name]
        except KeyError as exc:
            raise ValueError(f"Unknown broker {name!r}.") from exc

    def set_default(self, broker: AsyncBroker | None) -> None:
        """Set a validated default broker or clear the default."""
        if broker is not None:
            broker = self.resolve(broker)
        self._default_broker = broker

    def resolve(self, broker: AsyncBroker | None) -> AsyncBroker:
        """Resolve an explicit broker or return the default broker."""
        if isinstance(broker, str):
            raise TypeError(
                "Broker string references are not accepted here. "
                "Use router.get_broker(name) and pass the broker object.",
            )

        if broker is not None:
            for registered_broker in self._brokers.values():
                if registered_broker is broker:
                    return registered_broker
            raise ValueError("Broker is not registered in this router.")

        if self._default_broker is None:
            raise ValueError("Router doesn't have registered brokers.")
        return self._default_broker

    def default_flow(self, broker: AsyncBroker) -> FlowProtocol | None:
        """Return broker-owned default flow."""
        return getattr(broker, "default_flow", None)


class TaskRegistry:
    """Registry of tasks attached to one router."""

    def __init__(self) -> None:
        self._tasks: dict[str, AsyncTaskiqDecoratedTask[Any, Any]] = {}

    def register(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> None:
        """Register a decorated task and reject name conflicts."""
        self.ensure_available(task)
        self._tasks[task.task_name] = task

    def ensure_available(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any],
    ) -> None:
        """Validate that a task can be registered without mutating state."""
        existing_task = self._tasks.get(task.task_name)
        if existing_task is not None and existing_task is not task:
            self._raise_name_conflict(task.task_name)

    def ensure_name_available(self, task_name: str) -> None:
        """Validate that an unbound task name is available."""
        if task_name in self._tasks:
            self._raise_name_conflict(task_name)

    @staticmethod
    def _raise_name_conflict(task_name: str) -> None:
        raise ValueError(
            f"Task name {task_name!r} is already registered in this router.",
        )

    def find(
        self,
        task_name: str,
    ) -> AsyncTaskiqDecoratedTask[Any, Any] | None:
        """Find a task by name."""
        return self._tasks.get(task_name)

    def get_all(self) -> dict[str, AsyncTaskiqDecoratedTask[Any, Any]]:
        """Return all tasks registered in this registry."""
        return dict(self._tasks)
