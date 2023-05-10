import pytest

from taskiq import InMemoryBroker, SimpleRetryMiddleware


@pytest.mark.anyio
async def test_wait_result() -> None:
    """Tests wait_result."""

    broker = InMemoryBroker().with_middlewares(SimpleRetryMiddleware())
    runs = 0

    @broker.task()
    def run_task() -> str:
        nonlocal runs  # noqa: WPS420

        if runs == 0:
            runs += 1
            raise Exception("Retry")

        return "hello world!"

    task = await run_task.kiq()
    resp = await task.wait_result(timeout=1)

    assert resp.return_value == "hello world!"
