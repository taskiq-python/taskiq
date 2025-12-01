# mypy: disable-error-code="arg-type, union-attr"
import unittest
from unittest import mock

from opentelemetry import trace as trace_api
from opentelemetry.sdk import trace

from taskiq import TaskiqMessage
from taskiq.middlewares import opentelemetry_middleware

from .taskiq_test_tasks import broker


class TestUtils(unittest.TestCase):
    def setUp(self) -> None:
        self.broker = broker

    def test_set_attributes_from_context(self) -> None:
        # it should extract only relevant keys
        context = {
            "_retries": "4",
            "delay": "30",
            "max_retries": "6",
            "retry_on_error": "true",
            "timeout": "60",
            "X-Taskiq-requeue": "4",
            "custom_meta": "custom_value",
        }

        span = trace._Span("name", mock.Mock(spec=trace_api.SpanContext))
        opentelemetry_middleware.set_attributes_from_context(span, context)

        self.assertEqual(span.attributes.get("taskiq._retries"), "4")
        self.assertEqual(span.attributes.get("taskiq.delay"), "30")
        self.assertEqual(span.attributes.get("taskiq.max_retries"), "6")
        self.assertEqual(span.attributes.get("taskiq.retry_on_error"), "true")
        self.assertEqual(span.attributes.get("taskiq.timeout"), "60")
        self.assertEqual(span.attributes.get("taskiq.X-Taskiq-requeue"), "4")

        self.assertNotIn("custom_meta", span.attributes)

    def test_set_attributes_not_recording(self) -> None:
        # it should extract only relevant keys
        context = {
            "_retries": "4",
            "delay": "30",
            "max_retries": "6",
            "retry_on_error": "true",
            "timeout": "60",
            "X-Taskiq-requeue": "4",
            "custom_meta": "custom_value",
        }

        mock_span = mock.Mock()
        mock_span.is_recording.return_value = False
        opentelemetry_middleware.set_attributes_from_context(mock_span, context)
        self.assertFalse(mock_span.is_recording())
        self.assertTrue(mock_span.is_recording.called)
        self.assertFalse(mock_span.set_attribute.called)
        self.assertFalse(mock_span.set_status.called)

    def test_set_attributes_from_context_empty_keys(self) -> None:
        # it should not extract empty keys
        context = {
            "retries": 0,
        }

        span = trace._Span("name", mock.Mock(spec=trace_api.SpanContext))
        opentelemetry_middleware.set_attributes_from_context(span, context)

        self.assertEqual(len(span.attributes), 0)

    def test_span_propagation(self) -> None:
        # propagate and retrieve a Span
        message = mock.Mock(
            task_id="7c6731af-9533-40c3-83a9-25b58f0d837f",
            spec=TaskiqMessage,
        )
        span = trace._Span("name", mock.Mock(spec=trace_api.SpanContext))
        opentelemetry_middleware.attach_context(message, span, mock.Mock(), "")
        ctx = opentelemetry_middleware.retrieve_context(message)
        self.assertIsNotNone(ctx)
        span_after, _, _ = ctx  # type: ignore[misc]
        self.assertIs(span, span_after)

    def test_span_delete(self) -> None:
        # propagate a Span
        message = mock.Mock(
            task_id="7c6731af-9533-40c3-83a9-25b58f0d837f",
            spec=TaskiqMessage,
        )
        span = trace._Span("name", mock.Mock(spec=trace_api.SpanContext))
        opentelemetry_middleware.attach_context(message, span, mock.Mock(), "")
        # delete the Span
        opentelemetry_middleware.detach_context(message)
        self.assertEqual(opentelemetry_middleware.retrieve_context(message), None)

    def test_optional_message_span_attach(self) -> None:
        span = trace._Span("name", mock.Mock(spec=trace_api.SpanContext))

        # assert this is is a no-aop
        self.assertIsNone(
            opentelemetry_middleware.attach_context(None, span, mock.Mock(), ""),  # type: ignore[func-returns-value]
        )

    def test_span_delete_empty(self) -> None:
        # delete the Span
        message = mock.Mock(
            task_id="7c6731af-9533-40c3-83a9-25b58f0d837f",
            spec=TaskiqMessage,
        )
        try:
            opentelemetry_middleware.detach_context(message)
            self.assertEqual(opentelemetry_middleware.retrieve_context(message), None)
        except Exception as ex:  # pylint: disable=broad-except
            self.fail(f"Exception was raised: {ex}")
