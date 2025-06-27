import asyncio

import pytest

from taskiq import InMemoryBroker, SimpleRetryMiddleware, SmartRetryMiddleware
from taskiq.exceptions import NoResultError


@pytest.mark.anyio
async def test_wait_result() -> None:
    """Tests wait_result."""
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(no_result_on_retry=True),
    )
    runs = 0

    @broker.task(retry_on_error=True)
    async def run_task() -> str:
        nonlocal runs

        if runs == 0:
            runs += 1
            raise Exception("Retry")

        return "hello world!"

    task = await run_task.kiq()
    resp = await task.wait_result(0.1, timeout=1)
    assert runs == 1

    assert resp.return_value == "hello world!"


@pytest.mark.anyio
async def test_wait_result_error() -> None:
    """Tests wait_result."""
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(no_result_on_retry=False),
    )
    runs = 0
    lock = asyncio.Lock()

    @broker.task(retry_on_error=True)
    async def run_task() -> str:
        nonlocal runs, lock

        await lock.acquire()

        if runs == 0:
            runs += 1
            raise ValueError("Retry")

        return "hello world!"

    task = await run_task.kiq()
    resp = await task.wait_result(0.1, timeout=1)
    assert resp.is_err
    assert runs == 1

    broker.result_backend.results.pop(task.task_id)  # type: ignore
    lock.release()

    resp = await task.wait_result(timeout=1)
    assert resp.return_value == "hello world!"


@pytest.mark.anyio
async def test_wait_result_no_result() -> None:
    """Tests wait_result."""
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(no_result_on_retry=False),
    )
    done = asyncio.Event()
    runs = 0
    lock = asyncio.Lock()

    @broker.task(retry_on_error=True)
    async def run_task() -> str:
        nonlocal runs, done, lock

        await lock.acquire()

        if runs == 0:
            runs += 1
            raise ValueError("Retry")

        done.set()
        raise NoResultError

    task = await run_task.kiq()
    resp = await task.wait_result(0.1, timeout=1)
    with pytest.raises(ValueError):
        resp.raise_for_error()

    broker.result_backend.results.pop(task.task_id)  # type: ignore
    lock.release()

    assert await asyncio.wait_for(done.wait(), timeout=1)
    with pytest.raises(KeyError):
        await broker.result_backend.get_result(task.task_id)


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


@pytest.mark.anyio
async def test_retry_of_custom_exc_types_of_simple_middleware() -> None:
    # test that the passed error will be handled
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(
            no_result_on_retry=True,
            default_retry_label=True,
            types_of_exceptions=(KeyError, ValueError),
        )
    )
    runs = 0

    @broker.task(max_retries=10)
    def run_task() -> None:
        nonlocal runs

        runs += 1

        raise ValueError()

    task = await run_task.kiq()
    resp = await task.wait_result(timeout=1)
    with pytest.raises(ValueError):
        resp.raise_for_error()

    assert runs == 10

    # test that an untransmitted error will not be handled
    broker = InMemoryBroker().with_middlewares(
        SimpleRetryMiddleware(
            no_result_on_retry=True,
            default_retry_label=True,
            types_of_exceptions=(KeyError,),
        )
    )
    runs = 0

    @broker.task(max_retries=10)
    def run_task2() -> None:
        nonlocal runs

        runs += 1

        raise ValueError()

    task = await run_task2.kiq()
    resp = await task.wait_result(timeout=1)
    with pytest.raises(ValueError):
        resp.raise_for_error()

    assert runs == 1


@pytest.mark.anyio
async def test_retry_of_custom_exc_types_of_smart_middleware() -> None:
    # test that the passed error will be handled
    broker = InMemoryBroker().with_middlewares(
        SmartRetryMiddleware(
            no_result_on_retry=True,
            default_retry_label=True,
            types_of_exceptions=(KeyError, ValueError),
        )
    )
    runs = 0

    @broker.task(max_retries=10)
    def run_task() -> None:
        nonlocal runs

        runs += 1

        raise ValueError()

    task = await run_task.kiq()
    resp = await task.wait_result(timeout=1)
    with pytest.raises(ValueError):
        resp.raise_for_error()

    assert runs == 10

    # test that an untransmitted error will not be handled
    broker = InMemoryBroker().with_middlewares(
        SmartRetryMiddleware(
            no_result_on_retry=True,
            default_retry_label=True,
            types_of_exceptions=(KeyError,),
        )
    )
    runs = 0

    @broker.task(max_retries=10)
    def run_task2() -> None:
        nonlocal runs

        runs += 1

        raise ValueError()

    task = await run_task2.kiq()
    resp = await task.wait_result(timeout=1)
    with pytest.raises(ValueError):
        resp.raise_for_error()

    assert runs == 1
