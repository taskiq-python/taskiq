import asyncio
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

from opentelemetry import baggage, context
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import Span, SpanKind, StatusCode
from wrapt import wrap_function_wrapper

from taskiq import TaskiqResult
from taskiq.instrumentation import TaskiqInstrumentor
from taskiq.middlewares import opentelemetry_middleware

from .taskiq_test_tasks import (
    broker,
    task_add,
    task_raises,
    task_returns_baggage,
)


class TestTaskiqInstrumentation(TestBase):
    def tearDown(self) -> None:
        super().tearDown()
        TaskiqInstrumentor().uninstrument_broker(broker)

    def test_task(self) -> None:
        TaskiqInstrumentor().instrument_broker(broker)

        async def test() -> None:
            await task_add.kiq(1, 2)
            await broker.wait_all()

        asyncio.run(test())

        spans = self.sorted_spans(self.memory_exporter.get_finished_spans())
        self.assertEqual(len(spans), 2)

        consumer, producer = spans

        self.assertEqual(
            consumer.name,
            "execute/tests.opentelemetry.taskiq_test_tasks:task_add",
            f"{consumer._end_time}:{producer._end_time}",
        )
        self.assertEqual(consumer.kind, SpanKind.CONSUMER)
        self.assertSpanHasAttributes(
            consumer,
            {
                "taskiq.action": "execute",
                "taskiq.task_name": "tests.opentelemetry.taskiq_test_tasks:task_add",
            },
        )

        self.assertEqual(consumer.status.status_code, StatusCode.UNSET)

        self.assertEqual(0, len(consumer.events))

        self.assertEqual(
            producer.name,
            "send/tests.opentelemetry.taskiq_test_tasks:task_add",
        )
        self.assertEqual(producer.kind, SpanKind.PRODUCER)
        self.assertSpanHasAttributes(
            producer,
            {
                "taskiq.action": "send",
                "taskiq.task_name": "tests.opentelemetry.taskiq_test_tasks:task_add",
            },
        )

        self.assertNotEqual(consumer.parent, producer.context)
        self.assertEqual(consumer.parent.span_id, producer.context.span_id)
        self.assertEqual(consumer.context.trace_id, producer.context.trace_id)

    def test_task_raises(self) -> None:
        TaskiqInstrumentor().instrument_broker(broker)

        async def test() -> None:
            await task_raises.kiq()
            await broker.wait_all()

        asyncio.run(test())

        spans = self.sorted_spans(self.memory_exporter.get_finished_spans())
        self.assertEqual(len(spans), 2)

        consumer, producer = spans

        self.assertEqual(
            consumer.name,
            "execute/tests.opentelemetry.taskiq_test_tasks:task_raises",
        )
        self.assertEqual(consumer.kind, SpanKind.CONSUMER)
        self.assertSpanHasAttributes(
            consumer,
            {
                "taskiq.action": "execute",
                "taskiq.task_name": "tests.opentelemetry.taskiq_test_tasks:task_raises",
            },
        )

        self.assertEqual(consumer.status.status_code, StatusCode.ERROR)

        self.assertEqual(1, len(consumer.events))
        event = consumer.events[0]

        self.assertIn(SpanAttributes.EXCEPTION_STACKTRACE, event.attributes)

        self.assertEqual(
            event.attributes[SpanAttributes.EXCEPTION_TYPE],
            "tests.opentelemetry.taskiq_test_tasks.CustomError",
        )

        self.assertEqual(
            event.attributes[SpanAttributes.EXCEPTION_MESSAGE],
            "The task failed!",
        )

        self.assertEqual(
            producer.name,
            "send/tests.opentelemetry.taskiq_test_tasks:task_raises",
        )
        self.assertEqual(producer.kind, SpanKind.PRODUCER)
        self.assertSpanHasAttributes(
            producer,
            {
                "taskiq.action": "send",
                "taskiq.task_name": "tests.opentelemetry.taskiq_test_tasks:task_raises",
            },
        )

        self.assertNotEqual(consumer.parent, producer.context)
        self.assertEqual(consumer.parent.span_id, producer.context.span_id)
        self.assertEqual(consumer.context.trace_id, producer.context.trace_id)

    def test_uninstrument(self) -> None:
        TaskiqInstrumentor().instrument_broker(broker)
        TaskiqInstrumentor().uninstrument_broker(broker)

        async def test() -> None:
            await task_add.kiq(1, 2)
            await broker.wait_all()

        asyncio.run(test())

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 0)

    def test_baggage(self) -> None:
        TaskiqInstrumentor().instrument_broker(broker)

        async def test() -> TaskiqResult[Any]:
            task = await task_returns_baggage.kiq()
            return await task.wait_result(timeout=2)

        ctx = baggage.set_baggage("key", "value")
        context.attach(ctx)

        result = asyncio.run(test())

        self.assertEqual(result.return_value, {"key": "value"})

    def test_task_not_instrumented_does_not_raise(self) -> None:
        def _retrieve_context_wrapper_none_token(
            wrapped: Callable[
                [Any],
                tuple[Span, AbstractContextManager[Span], object | None] | None,
            ],
            instance: Any,
            args: Any,
            kwargs: Any,
        ) -> tuple[Span, AbstractContextManager[Span], None] | None:
            ctx = wrapped(*args, **kwargs)
            if ctx is None:
                return ctx
            span, activation, _ = ctx
            return span, activation, None

        wrap_function_wrapper(
            opentelemetry_middleware,
            "retrieve_context",
            _retrieve_context_wrapper_none_token,
        )

        TaskiqInstrumentor().instrument_broker(broker)

        async def test() -> TaskiqResult[float]:
            task = await task_add.kiq(1, 2)
            return await task.wait_result(timeout=2)

        result = asyncio.run(test())
        spans = self.sorted_spans(self.memory_exporter.get_finished_spans())
        self.assertEqual(len(spans), 2)

        self.assertTrue(result.return_value)

        unwrap(opentelemetry_middleware, "retrieve_context")
