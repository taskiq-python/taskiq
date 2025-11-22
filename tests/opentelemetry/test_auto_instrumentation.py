import asyncio

from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import SpanKind, StatusCode

from taskiq import InMemoryBroker
from taskiq.instrumentation import TaskiqInstrumentor


class TestTaskiqAutoInstrumentation(TestBase):
    def test_auto_instrument(self) -> None:
        TaskiqInstrumentor().instrument()

        broker = InMemoryBroker(await_inplace=True)

        @broker.task
        async def task_add(a: float, b: float) -> float:
            return a + b

        async def test() -> None:
            await task_add.kiq(1, 2)
            await broker.wait_all()

        asyncio.run(test())

        spans = self.sorted_spans(self.memory_exporter.get_finished_spans())
        self.assertEqual(len(spans), 2)

        consumer, producer = spans

        self.assertEqual(
            consumer.name,
            "execute/tests.opentelemetry.test_auto_instrumentation:task_add",
            f"{consumer._end_time}:{producer._end_time}",
        )
        self.assertEqual(consumer.kind, SpanKind.CONSUMER)
        self.assertSpanHasAttributes(
            consumer,
            {
                "taskiq.action": "execute",
                "taskiq.task_name": "tests.opentelemetry.test_auto_instrumentation:task_add",  # noqa: E501
            },
        )

        self.assertEqual(consumer.status.status_code, StatusCode.UNSET)

        self.assertEqual(0, len(consumer.events))

        self.assertEqual(
            producer.name,
            "send/tests.opentelemetry.test_auto_instrumentation:task_add",
        )
        self.assertEqual(producer.kind, SpanKind.PRODUCER)
        self.assertSpanHasAttributes(
            producer,
            {
                "taskiq.action": "send",
                "taskiq.task_name": "tests.opentelemetry.test_auto_instrumentation:task_add",  # noqa: E501
            },
        )

        self.assertNotEqual(consumer.parent, producer.context)
        self.assertEqual(consumer.parent.span_id, producer.context.span_id)
        self.assertEqual(consumer.context.trace_id, producer.context.trace_id)
