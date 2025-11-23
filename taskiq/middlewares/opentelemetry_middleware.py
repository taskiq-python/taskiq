import logging
from contextlib import AbstractContextManager
from importlib.metadata import version
from typing import Any, TypeVar

from packaging.version import Version, parse

try:
    import opentelemetry  # noqa: F401
except ImportError as exc:
    raise ImportError(
        "Cannot import opentelemetry_middleware. "
        "Please install 'taskiq[opentelemetry]'.",
    ) from exc


from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.metrics import Meter, MeterProvider, get_meter
from opentelemetry.propagate import extract, inject
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Span, Tracer, TracerProvider
from opentelemetry.trace.status import Status, StatusCode

from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult, __version__

logger = logging.getLogger("taskiq.opentelemetry")

T = TypeVar("T")

# Taskiq Context key
CTX_KEY = "__otel_task_span"

# unlike pydantic v2, v1 includes CTX_KEY by default
# excluding it here
PYDANTIC_VER = parse(version("pydantic"))
IS_PYDANTIC1 = Version("2.0") > PYDANTIC_VER
if IS_PYDANTIC1:
    if TaskiqMessage.__exclude_fields__:  # type: ignore[attr-defined]
        TaskiqMessage.__exclude_fields__.update(CTX_KEY)  # type: ignore
    else:
        TaskiqMessage.__exclude_fields__ = {CTX_KEY}  # type: ignore

# Taskiq Context attributes
TASKIQ_CONTEXT_ATTRIBUTES = [
    "_retries",
    "delay",
    "max_retries",
    "retry_on_error",
    "timeout",
    "X-Taskiq-requeue",
]

# Task operations
_TASK_TAG_KEY = "taskiq.action"
_TASK_SEND = "send"
_TASK_EXECUTE = "execute"

_TASK_RETRY_REASON_KEY = "taskiq.retry.reason"
_TASK_NAME_KEY = "taskiq.task_name"


def set_attributes_from_context(span: Span, context: dict[str, Any]) -> None:
    """Helper to extract meta values from a Taskiq Context."""
    if not span.is_recording():
        return

    for key in TASKIQ_CONTEXT_ATTRIBUTES:
        value = context.get(key)

        # Skip this key if it is not set
        if value is None:
            continue

        # Skip `retries` if it's value is `0`
        if key == "_retries" and value == "0":
            continue

        attribute_name = f"taskiq.{key}"

        span.set_attribute(attribute_name, value)


def attach_context(
    message: TaskiqMessage | None,
    span: Span,
    activation: AbstractContextManager[Span],
    token: object | None,
    is_publish: bool = False,
) -> None:
    """
    Propagate context to `TaskiqMessage`.

    Helper to propagate a `Span`, `ContextManager` and context token
    for the given `Task` instance. This function uses a `dict` that stores
    the Span using the `(task_id, is_publish)` as a key. This is useful
    when information must be propagated from one Celery signal to another.

    We use (task_id, is_publish) for the key to ensure that publishing a
    task from within another task does not cause any conflicts.

    This mostly happens when either a task fails and a retry policy is in place,
    we end up trying to publish a task with the same id as the task currently running.
    """
    if message is None:
        return

    ctx_dict = getattr(message, CTX_KEY, None)

    if ctx_dict is None:
        ctx_dict = {}
        # use object.__setattr__ directly
        # to skip pydantic v1 setattr
        object.__setattr__(message, CTX_KEY, ctx_dict)

    ctx_dict[(message.task_id, is_publish)] = (span, activation, token)


def detach_context(message: TaskiqMessage, is_publish: bool = False) -> None:
    """Remove context from `TaskiqMessage`."""
    span_dict = getattr(message, CTX_KEY, None)
    if span_dict is None:
        return

    # See note in `attach_context` for key info
    span_dict.pop((message.task_id, is_publish), None)


def retrieve_context(
    message: TaskiqMessage,
    is_publish: bool = False,
) -> tuple[Span, AbstractContextManager[Span], object | None] | None:
    """Retrieve context from `TaskiqMessage`."""
    span_dict = getattr(message, CTX_KEY, None)
    if span_dict is None:
        return None

    # See note in `attach_context` for key info
    return span_dict.get((message.task_id, is_publish), None)


class OpenTelemetryMiddleware(TaskiqMiddleware):
    """Middleware to instrument Taskiq with OpenTelemetry."""

    def __init__(
        self,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        tracer: Tracer | None = None,
        meter: Meter | None = None,
    ) -> None:
        super().__init__()
        self._tracer = (
            trace.get_tracer(
                __name__,
                __version__,
                tracer_provider,
                schema_url="https://opentelemetry.io/schemas/1.11.0",
            )
            if tracer is None
            else tracer
        )
        self._meter = (
            get_meter(
                __name__,
                __version__,
                meter_provider,
                schema_url="https://opentelemetry.io/schemas/1.11.0",
            )
            if meter is None
            else meter
        )

    def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        """
        This function starts new span and propagates opentelemetry state to labels.

        :param message: current message.
        :return: message
        """
        logger.debug("pre_send task_id=%s", message.task_id)

        operation_name = f"{_TASK_SEND}/{message.task_name}"
        span = self._tracer.start_span(operation_name, kind=trace.SpanKind.PRODUCER)

        if span.is_recording():
            span.set_attribute(_TASK_TAG_KEY, _TASK_SEND)
            span.set_attribute(SpanAttributes.MESSAGING_MESSAGE_ID, message.task_id)
            span.set_attribute(_TASK_NAME_KEY, message.task_name)
            set_attributes_from_context(span, message.labels)

        activation = trace.use_span(span, end_on_exit=True)
        activation.__enter__()
        attach_context(message, span, activation, None, is_publish=True)
        inject(message.labels)

        return message

    def post_send(self, message: TaskiqMessage) -> None:
        """
        This function closes span from `pre_send`.

        :param message: current message.
        """
        logger.debug("post_send task_id=%s", message.task_id)
        # retrieve and finish the Span
        ctx = retrieve_context(message, is_publish=True)

        if ctx is None:
            logger.debug("no existing span found for task_id=%s", message.task_id)
            return

        _, activation, _ = ctx

        activation.__exit__(None, None, None)
        detach_context(message, is_publish=True)

    def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        """
        This function starts new span and propagates opentelemetry state to labels.

        :param message: current message.
        :return: message
        """
        logger.debug("pre_execute task_id=%s", message.task_id)
        tracectx = extract(message.labels) or None
        token = context_api.attach(tracectx) if tracectx is not None else None

        operation_name = f"{_TASK_EXECUTE}/{message.task_name}"
        span = self._tracer.start_span(
            operation_name,
            context=tracectx,
            kind=trace.SpanKind.CONSUMER,
        )

        activation = trace.use_span(span, end_on_exit=True)
        activation.__enter__()  # pylint: disable=E1101
        attach_context(message, span, activation, token)
        return message

    def post_save(  # pylint: disable=R6301
        self,
        message: TaskiqMessage,
        result: TaskiqResult[T],
    ) -> None:
        """
        This function closes span from `pre_execute`.

        :param message: received message.
        :param result: result of the execution.
        """
        logger.debug("post_execute task_id=%s", message.task_id)

        # retrieve and finish the Span
        ctx = retrieve_context(message)

        if ctx is None:
            logger.warning("no existing span found for task_id=%s", message.task_id)
            return

        span, activation, token = ctx

        if span.is_recording():
            span.set_attribute(_TASK_TAG_KEY, _TASK_EXECUTE)
            set_attributes_from_context(span, message.labels)
            span.set_attribute(_TASK_NAME_KEY, message.task_name)

        activation.__exit__(None, None, None)
        detach_context(message)
        # if the process sending the task is not instrumented
        # there's no incoming context and no token to detach
        if token is not None:
            context_api.detach(token)  # type: ignore[arg-type]

    def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[T],
        exception: BaseException,
    ) -> None:
        """
        This function closes span from `pre_execute` with error.

        :param message: Message that caused the error.
        :param result: execution result.
        :param exception: found exception.
        """
        ctx = retrieve_context(message)

        if ctx is None:
            return

        span, _, _ = ctx

        if not span.is_recording():
            return

        retry_on_error = message.labels.get("retry_on_error")
        if isinstance(retry_on_error, str):
            retry_on_error = retry_on_error.lower() == "true"

        if retry_on_error is None:
            retry_on_error = False

        if retry_on_error:
            # Add retry reason metadata to span
            span.set_attribute(_TASK_RETRY_REASON_KEY, str(exception))
            return

        status_kwargs = {
            "status_code": StatusCode.ERROR,
            "description": str(exception),
        }
        span.record_exception(exception)
        span.set_status(Status(**status_kwargs))  # type: ignore[arg-type]
