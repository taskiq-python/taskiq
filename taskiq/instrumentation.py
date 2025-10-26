"""
Instrument `taskiq`_ to trace Taskiq applications.

.. _taskiq: https://pypi.org/project/taskiq/

Usage
-----

* Run instrumented task

.. code:: python

    import asyncio

    from taskiq import InMemoryBroker, TaskiqEvents, TaskiqState
    from taskiq.instrumentation import TaskiqInstrumentor

    broker = InMemoryBroker()

    @broker.on_event(TaskiqEvents.WORKER_STARTUP)
    async def startup(state: TaskiqState) -> None:
        TaskiqInstrumentor().instrument()

    @broker.task
    async def add(x, y):
        return x + y

    async def main():
        await broker.startup()
        await my_task.kiq(1, 2)
        await broker.shutdown()

    if __name__ == "__main__":
        asyncio.run(main())

API
---
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Collection, Optional
from weakref import WeakSet as _WeakSet

try:
    import opentelemetry  # noqa: F401
except ImportError as exc:
    raise ImportError(
        "Cannot instrument. Please install 'taskiq[opentelemetry]'.",
    ) from exc


from opentelemetry.instrumentation.instrumentor import (  # type: ignore[attr-defined]
    BaseInstrumentor,
)
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import TracerProvider
from wrapt import wrap_function_wrapper

from taskiq import AsyncBroker
from taskiq.middlewares.opentelemetry_middleware import OpenTelemetryMiddleware

if TYPE_CHECKING:
    pass

logger = logging.getLogger("taskiq.opentelemetry")


class TaskiqInstrumentor(BaseInstrumentor):
    """OpenTelemetry instrumentor for Taskiq."""

    _instrumented_brokers: _WeakSet[AsyncBroker] = _WeakSet()

    def __init__(self) -> None:
        super().__init__()
        self._middleware = None

    def instrument_broker(
        self,
        broker: AsyncBroker,
        tracer_provider: Optional[TracerProvider] = None,
        meter_provider: Optional[MeterProvider] = None,
    ) -> None:
        """Instrument broker."""
        if not hasattr(broker, "_is_instrumented_by_opentelemetry"):
            broker._is_instrumented_by_opentelemetry = False  # type: ignore[attr-defined] # noqa: SLF001

        if not getattr(broker, "is_instrumented_by_opentelemetry", False):
            broker.middlewares.insert(
                0,
                OpenTelemetryMiddleware(
                    tracer_provider=tracer_provider,
                    meter_provider=meter_provider,
                ),
            )
            broker._is_instrumented_by_opentelemetry = True  # type: ignore[attr-defined] # noqa: SLF001
            if broker not in self._instrumented_brokers:
                self._instrumented_brokers.add(broker)
        else:
            logger.warning(
                "Attempting to instrument taskiq broker while already instrumented",
            )

    def uninstrument_broker(self, broker: AsyncBroker) -> None:
        """Uninstrument broker."""
        broker.middlewares = [
            middleware
            for middleware in broker.middlewares
            if not isinstance(middleware, OpenTelemetryMiddleware)
        ]
        broker._is_instrumented_by_opentelemetry = False  # type: ignore[attr-defined] # noqa: SLF001
        self._instrumented_brokers.discard(broker)

    def instrumentation_dependencies(self) -> Collection[str]:
        """This function tells which library this instrumentor instruments."""
        return ("taskiq >= 0.0.1",)

    def _instrument(self, **kwargs: Any) -> None:
        def broker_init(
            init: Callable[[Any], Any],
            broker: AsyncBroker,
            args: Any,
            kwargs: Any,
        ) -> None:
            result = init(*args, **kwargs)
            self.instrument_broker(broker)
            return result

        wrap_function_wrapper("taskiq", "AsyncBroker.__init__", broker_init)

    def _uninstrument(self, **kwargs: Any) -> None:
        instances_to_uninstrument = list(self._instrumented_brokers)
        for broker in instances_to_uninstrument:
            self.uninstrument_broker(broker)
        self._instrumented_brokers.clear()
        unwrap(AsyncBroker, "__init__")
