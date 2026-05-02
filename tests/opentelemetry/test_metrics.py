import asyncio
from typing import Any

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.test.test_base import TestBase

from taskiq.instrumentation import TaskiqInstrumentor
from taskiq.middlewares.opentelemetry_middleware import OpenTelemetryMiddleware

from .taskiq_test_tasks import (
    broker,
    task_add,
    task_does_processing,
    task_raises,
)


class TestTaskiqOTelMetrics(TestBase):
    def setUp(self) -> None:
        super().setUp()
        self.reader = InMemoryMetricReader()
        self.meter_provider = MeterProvider(metric_readers=[self.reader])
        TaskiqInstrumentor().instrument_broker(
            broker,
            meter_provider=self.meter_provider,
        )

    def tearDown(self) -> None:
        super().tearDown()
        TaskiqInstrumentor().uninstrument_broker(broker)

    def _get_data_points(self, metric_name: str) -> list[Any]:
        metrics = self.reader.get_metrics_data()
        if metrics is None:
            return []
        return [
            point
            for rm in metrics.resource_metrics
            for sm in rm.scope_metrics
            for metric in sm.metrics
            if metric.name == metric_name
            for point in metric.data.data_points
        ]

    def test_metrics_exist(self) -> None:
        async def test() -> None:
            await task_add.kiq(1, 2)
            await task_raises.kiq()
            await broker.wait_all()

        asyncio.run(test())

        metrics = self.reader.get_metrics_data()
        self.assertIsNotNone(metrics)
        expected = {
            "task_errors",
            "tasks_sent",
            "task_success",
            "task_execution_time",
            "task_wait_time",
            "worker_active_tasks",
        }
        found = {
            metric.name
            for rm in metrics.resource_metrics  # type: ignore[union-attr]
            for sm in rm.scope_metrics
            for metric in sm.metrics
        }
        self.assertSetEqual(found.intersection(expected), expected)

    def test_success_counter(self) -> None:
        async def test() -> None:
            for _ in range(3):
                await task_add.kiq(1, 2)
            await broker.wait_all()

        asyncio.run(test())

        points = self._get_data_points("task_success")
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].value, 3)

    def test_error_counter_no_retry(self) -> None:
        async def test() -> None:
            for _ in range(3):
                await task_raises.kiq()
            await broker.wait_all()

        asyncio.run(test())

        points = self._get_data_points("task_errors")
        no_retry_points = [
            p for p in points if p.attributes.get("retry_error") is False
        ]
        self.assertEqual(len(no_retry_points), 1)
        self.assertEqual(no_retry_points[0].value, 3)

    def test_error_counter_with_retry(self) -> None:
        async def test() -> None:
            for _ in range(3):
                await task_raises.kicker().with_labels(retry_on_error="true").kiq()
            await broker.wait_all()

        asyncio.run(test())

        points = self._get_data_points("task_errors")
        retry_points = [p for p in points if p.attributes.get("retry_error") is True]
        self.assertEqual(len(retry_points), 1)
        self.assertEqual(retry_points[0].value, 3)

    def test_execution_time_histogram(self) -> None:
        async def test() -> None:
            for _ in range(3):
                await task_does_processing.kiq(0.01)
            await broker.wait_all()

        asyncio.run(test())

        points = self._get_data_points("task_execution_time")
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].count, 3)
        self.assertGreater(points[0].sum, 0)

    def test_task_wait_time_histogram(self) -> None:
        async def test() -> None:
            await task_does_processing.kiq(0.01)
            await broker.wait_all()

        asyncio.run(test())

        points = self._get_data_points("task_wait_time")
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].count, 1)
        self.assertGreaterEqual(points[0].sum, 0)

    def test_queue_time(self) -> None:
        async def test() -> None:
            for _ in range(3):
                await task_add.kiq(1, 2)
            await broker.wait_all()

        asyncio.run(test())

        points = self._get_data_points("task_wait_time")
        # all 3 tasks share the same task_name so they aggregate into one data point
        self.assertEqual(len(points), 1)
        point = points[0]
        # 3 tasks recorded
        self.assertEqual(point.count, 3)
        # queue time must be non-negative — a negative value means timestamps
        # were not written/read correctly
        self.assertGreaterEqual(point.sum, 0)
        self.assertGreaterEqual(point.min, 0)
        # task_name attribute must be present and correct
        self.assertEqual(
            point.attributes.get("task_name"),
            "tests.opentelemetry.taskiq_test_tasks:task_add",
        )

    def test_tasks_sent_counter(self) -> None:
        async def test() -> None:
            for _ in range(3):
                await task_add.kiq(1, 2)
            await broker.wait_all()

        asyncio.run(test())

        points = self._get_data_points("tasks_sent")
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].value, 3)

    def test_active_tasks_counter(self) -> None:
        async def test() -> None:
            for _ in range(3):
                await task_add.kiq(1, 2)
            await broker.wait_all()

        asyncio.run(test())

        points = self._get_data_points("worker_active_tasks")
        # all 3 tasks share the same task_name so they aggregate into one data point
        self.assertEqual(len(points), 1)
        # net zero: pre_execute incremented, post_execute decremented for each task
        self.assertEqual(points[0].value, 0)
        self.assertIn("task_name", points[0].attributes)
        self.assertEqual(
            points[0].attributes.get("task_name"),
            "tests.opentelemetry.taskiq_test_tasks:task_add",
        )

    def test_prefetch_queue_counter(self) -> None:
        middleware = next(
            m for m in broker.middlewares if isinstance(m, OpenTelemetryMiddleware)
        )
        middleware.on_prefetch_queue_add()
        middleware.on_prefetch_queue_add()
        middleware.on_prefetch_queue_add()
        middleware.on_prefetch_queue_remove()

        points = self._get_data_points("worker_prefetched_tasks")
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].value, 2)

    def test_worker_resource_metrics_when_worker_process(self) -> None:
        middleware = next(
            m for m in broker.middlewares if isinstance(m, OpenTelemetryMiddleware)
        )
        middleware.set_broker(broker)
        broker.is_worker_process = True
        try:
            metrics_data = self.reader.get_metrics_data()
            self.assertIsNotNone(metrics_data)
            found = {
                metric.name
                for rm in metrics_data.resource_metrics  # type: ignore[union-attr]
                for sm in rm.scope_metrics
                for metric in sm.metrics
            }
            self.assertIn("worker_cpu_utilization", found)
            self.assertIn("worker_memory_utilization", found)
        finally:
            broker.is_worker_process = False
            middleware.set_broker(None)  # type: ignore[arg-type]
