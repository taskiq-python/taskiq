from collections.abc import Iterable
from typing import Any

import pytest

from taskiq import (
    InMemoryBroker,
    SimpleRetryMiddleware,
    SmartRetryMiddleware,
    TaskiqMessage,
    TaskiqResult,
)
from taskiq.message import BrokerMessage


class RecordingBroker(InMemoryBroker):
    def __init__(self) -> None:
        super().__init__()
        self.sent: list[BrokerMessage] = []

    async def kick(self, message: BrokerMessage) -> None:
        self.sent.append(message)


@pytest.mark.parametrize(
    "middleware_class",
    [SimpleRetryMiddleware, SmartRetryMiddleware],
)
@pytest.mark.parametrize("filter_kind", ["generator", "list", "empty", "none"])
async def test_exception_filter_is_reusable(
    middleware_class: type[SimpleRetryMiddleware] | type[SmartRetryMiddleware],
    filter_kind: str,
) -> None:
    exception_types: Iterable[type[BaseException]] | None
    if filter_kind == "generator":
        exception_types = (kind for kind in [ValueError])
    elif filter_kind == "list":
        exception_types = [ValueError]
    elif filter_kind == "empty":
        exception_types = iter([])
    else:
        exception_types = None

    broker = RecordingBroker()
    middleware = middleware_class(
        default_retry_label=True,
        types_of_exceptions=exception_types,
    )
    broker.with_middlewares(middleware)
    try:
        for task_id in ["first", "second"]:
            error = ValueError("retry")
            message = TaskiqMessage(
                task_id=task_id,
                task_name="test",
                labels={},
                args=[],
                kwargs={},
            )
            result: TaskiqResult[Any] = TaskiqResult(
                is_err=True,
                log=None,
                return_value=None,
                execution_time=0,
                error=error,
            )
            await middleware.on_error(message, result, error)
        assert len(broker.sent) == (0 if filter_kind == "empty" else 2)
    finally:
        await broker.shutdown()


@pytest.mark.parametrize(
    "middleware_class",
    [SimpleRetryMiddleware, SmartRetryMiddleware],
)
async def test_nonmatching_error_does_not_consume_filter(
    middleware_class: type[SimpleRetryMiddleware] | type[SmartRetryMiddleware],
) -> None:
    broker = RecordingBroker()
    middleware = middleware_class(
        default_retry_label=True,
        types_of_exceptions=(kind for kind in [ValueError]),
    )
    broker.with_middlewares(middleware)
    try:
        for error in [TypeError("skip"), ValueError("retry")]:
            message = TaskiqMessage(
                task_id=str(type(error)),
                task_name="test",
                labels={},
                args=[],
                kwargs={},
            )
            result: TaskiqResult[Any] = TaskiqResult(
                is_err=True,
                log=None,
                return_value=None,
                execution_time=0,
                error=error,
            )
            await middleware.on_error(message, result, error)
        assert len(broker.sent) == 1
    finally:
        await broker.shutdown()
