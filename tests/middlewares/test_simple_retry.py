from taskiq.message import TaskiqMessage
from taskiq.middlewares.simple_retry_middleware import SimpleRetryMiddleware
from taskiq.result import TaskiqResult
from tests.utils import AsyncQueueBroker


async def test_successful_retry() -> None:
    broker = AsyncQueueBroker()
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
    resend = broker.formatter.loads(broker.queue.get_nowait())
    assert resend.task_name == "meme"
    assert resend.labels["_retries"] == "1"


async def test_no_retry() -> None:
    broker = AsyncQueueBroker()
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
    assert broker.queue.empty()


async def test_max_retries() -> None:
    broker = AsyncQueueBroker()
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
    assert broker.queue.empty()
