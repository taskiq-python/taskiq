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

    @broker.task
    async def add(x, y):
        return x + y

    async def main():
        TaskiqInstrumentor().instrument()
        await broker.startup()
        await my_task.kiq(1, 2)
        await broker.shutdown()

    if __name__ == "__main__":
        asyncio.run(main())

API
---
"""


import logging
from functools import partial
from typing import Any, Callable, Collection, Optional
from weakref import WeakSet as _WeakSet

from taskiq.cli.worker.args import WorkerArgs

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
from wrapt import wrap_function_wrapper, wrap_object_attribute

from taskiq import AsyncBroker
from taskiq.cli.worker.process_manager import ProcessManager
from taskiq.middlewares.opentelemetry_middleware import OpenTelemetryMiddleware

logger = logging.getLogger("taskiq.opentelemetry")


def _worker_function_with_sitecustomize(
    worker_function: Callable[[WorkerArgs], None],
    *args: Any,
    **kwargs: Any,
) -> None:
    import opentelemetry.instrumentation.auto_instrumentation.sitecustomize  # noqa

    return worker_function(*args, **kwargs)


def _worker_function_factory(
    worker_function: Callable[[WorkerArgs], None],
) -> Callable[[WorkerArgs], None]:
    return partial(_worker_function_with_sitecustomize, worker_function)


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

        wrap_function_wrapper("taskiq.abc.broker", "AsyncBroker.__init__", broker_init)
        wrap_object_attribute(
            "taskiq.cli.worker.process_manager",
            "ProcessManager.worker_function",
            _worker_function_factory,
        )

    def _uninstrument(self, **kwargs: Any) -> None:
        instances_to_uninstrument = list(self._instrumented_brokers)
        for broker in instances_to_uninstrument:
            self.uninstrument_broker(broker)
        self._instrumented_brokers.clear()
        unwrap(AsyncBroker, "__init__")
        delattr(ProcessManager, "worker_function")
