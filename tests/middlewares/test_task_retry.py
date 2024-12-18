import asyncio
import platform
import time

import pytest

from taskiq import InMemoryBroker, SimpleRetryMiddleware
from taskiq.exceptions import NoResultError


@pytest.mark.anyio
async def test_wait_result() -> None:
    """Tests wait_result."""
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(no_result_on_retry=True),
    )
    runs = 0

    @broker.task(retry_on_error=True)
    def run_task() -> str:
        nonlocal runs

        if runs == 0:
            runs += 1
            raise Exception("Retry")

        time.sleep(0.2)
        return "hello world!"

    task = await run_task.kiq()
    resp = await task.wait_result(0.1, timeout=1)

    assert resp.return_value == "hello world!"


@pytest.mark.anyio
@pytest.mark.skipif(
    platform.system().lower() == "darwin",
    reason="Not supported on macOS",
)
async def test_wait_result_error() -> None:
    """Tests wait_result."""
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(no_result_on_retry=False),
    )
    runs = 0

    @broker.task(retry_on_error=True)
    def run_task() -> str:
        nonlocal runs

        if runs == 0:
            runs += 1
            raise ValueError("Retry")

        time.sleep(0.2)
        return "hello world!"

    task = await run_task.kiq()
    resp = await task.wait_result(0.1, timeout=1)
    with pytest.raises(ValueError):
        resp.raise_for_error()

    await asyncio.sleep(0.2)
    resp = await task.wait_result(timeout=1)
    assert resp.return_value == "hello world!"


@pytest.mark.anyio
async def test_wait_result_no_result() -> None:
    """Tests wait_result."""
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(no_result_on_retry=False),
    )
    done = False
    runs = 0

    @broker.task(retry_on_error=True)
    def run_task() -> str:
        nonlocal runs, done

        if runs == 0:
            runs += 1
            raise ValueError("Retry")

        time.sleep(0.2)
        done = True
        raise NoResultError

    task = await run_task.kiq()
    resp = await task.wait_result(0.1, timeout=1)
    with pytest.raises(ValueError):
        resp.raise_for_error()

    await asyncio.sleep(0.2)
    resp = await task.wait_result(timeout=1)
    with pytest.raises(ValueError):
        resp.raise_for_error()

    assert done


@pytest.mark.anyio
async def test_max_retries() -> None:
    """Tests wait_result."""
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(
            no_result_on_retry=True,
            default_retry_label=True,
        ),
    )
    runs = 0

    @broker.task(max_retries=10)
    def run_task() -> str:
        nonlocal runs

        runs += 1
        raise ValueError(runs)

    task = await run_task.kiq()
    resp = await task.wait_result(timeout=1)
    with pytest.raises(ValueError):
        resp.raise_for_error()

    assert runs == 10
    assert str(resp.error) == str(runs)


@pytest.mark.anyio
async def test_no_retry() -> None:
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(
            no_result_on_retry=True,
            default_retry_label=True,
        ),
    )
    runs = 0

    @broker.task(retry_on_error=False, max_retries=10)
    def run_task() -> str:
        nonlocal runs

        runs += 1
        raise ValueError(runs)

    task = await run_task.kiq()
    resp = await task.wait_result(timeout=1)
    with pytest.raises(ValueError):
        resp.raise_for_error()

    assert runs == 1
    assert str(resp.error) == str(runs)
