import uuid
from unittest.mock import AsyncMock

import pytest

from taskiq.formatters.json_formatter import JSONFormatter
from taskiq.message import TaskiqMessage
from taskiq.middlewares.simple_retry_middleware import SimpleRetryMiddleware
from taskiq.result import TaskiqResult


@pytest.fixture
def broker() -> AsyncMock:
    mocked_broker = AsyncMock()
    mocked_broker.id_generator = lambda: uuid.uuid4().hex
    mocked_broker.formatter = JSONFormatter()
    return mocked_broker


async def test_successful_retry(broker: AsyncMock) -> None:
    middleware = SimpleRetryMiddleware()
    middleware.set_broker(broker)
    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="meme",
            labels={
                "retry_on_error": "True",
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception(),
    )
    resend: TaskiqMessage = broker.kick.await_args.args[0]
    assert resend.task_name == "meme"
    assert resend.labels["_retries"] == "1"


async def test_no_retry(broker: AsyncMock) -> None:
    middleware = SimpleRetryMiddleware()
    middleware.set_broker(broker)
    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="meme",
            labels={},
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception(),
    )
    broker.kick.assert_not_called()


async def test_max_retries(broker: AsyncMock) -> None:
    middleware = SimpleRetryMiddleware(default_retry_count=3)
    middleware.set_broker(broker)
    await middleware.on_error(
        TaskiqMessage(
            task_id="test_id",
            task_name="meme",
            labels={
                "retry_on_error": "True",
                "_retries": "2",
            },
            args=[],
            kwargs={},
        ),
        TaskiqResult(is_err=True, return_value=None, execution_time=0.0),
        Exception(),
    )
    broker.kick.assert_not_called()
