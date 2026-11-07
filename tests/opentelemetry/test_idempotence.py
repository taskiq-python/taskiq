from collections.abc import Generator

import pytest
from opentelemetry import context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from taskiq import InMemoryBroker
from taskiq.instrumentation import TaskiqInstrumentor
from taskiq.middlewares.opentelemetry_middleware import OpenTelemetryMiddleware


@pytest.fixture
def local_broker() -> Generator[InMemoryBroker, None, None]:
    broker = InMemoryBroker(await_inplace=True)
    TaskiqInstrumentor().uninstrument_broker(broker)
    try:
        yield broker
    finally:
        TaskiqInstrumentor().uninstrument_broker(broker)
        broker.executor.shutdown()


def test_repeated_instrumentation_keeps_one_middleware(
    local_broker: InMemoryBroker,
) -> None:
    instrumentor = TaskiqInstrumentor()
    instrumentor.instrument_broker(local_broker)
    first = local_broker.middlewares[0]
    instrumentor.instrument_broker(local_broker)
    assert local_broker.middlewares == [first]


def test_uninstrumented_broker_can_be_instrumented_again(
    local_broker: InMemoryBroker,
) -> None:
    instrumentor = TaskiqInstrumentor()
    instrumentor.instrument_broker(local_broker)
    instrumentor.uninstrument_broker(local_broker)
    assert local_broker.middlewares == []
    instrumentor.instrument_broker(local_broker)
    assert len(local_broker.middlewares) == 1
    assert isinstance(local_broker.middlewares[0], OpenTelemetryMiddleware)


def test_different_brokers_are_instrumented_independently(
    local_broker: InMemoryBroker,
) -> None:
    other = InMemoryBroker()
    instrumentor = TaskiqInstrumentor()
    try:
        instrumentor.instrument_broker(local_broker)
        instrumentor.instrument_broker(other)
        assert len(local_broker.middlewares) == len(other.middlewares) == 1
        assert local_broker.middlewares[0] is not other.middlewares[0]
    finally:
        instrumentor.uninstrument_broker(other)
        other.executor.shutdown()


async def test_repeated_setup_does_not_duplicate_task_spans(
    local_broker: InMemoryBroker,
) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    instrumentor = TaskiqInstrumentor()
    token = context.attach(context.Context())
    try:
        instrumentor.instrument_broker(local_broker, tracer_provider=provider)
        instrumentor.instrument_broker(local_broker, tracer_provider=provider)

        @local_broker.task
        async def task() -> int:
            return 42

        result = await task.kiq()
        assert (await result.get_result()).return_value == 42
        spans = exporter.get_finished_spans()
        assert len([span for span in spans if span.kind == SpanKind.PRODUCER]) == 1
        assert len([span for span in spans if span.kind == SpanKind.CONSUMER]) == 1
    finally:
        instrumentor.uninstrument_broker(local_broker)
        context.detach(token)
        provider.shutdown()
