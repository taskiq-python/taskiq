import asyncio
from typing import Any

import pytest

from taskiq import InMemoryBroker
from taskiq.brokers.inmemory_broker import InmemoryResultBackend
from taskiq.exceptions import TaskiqResultTimeoutError
from taskiq.funcs import gather
from taskiq.result import TaskiqResult
from taskiq.task import AsyncTaskiqTask


@pytest.mark.parametrize("reverse", [False, True])
async def test_gather_keeps_results_from_independent_brokers(reverse: bool) -> None:
    """Parallel branches can use the same business ID in separate result stores."""
    research_broker = InMemoryBroker(await_inplace=True)
    review_broker = InMemoryBroker(await_inplace=True)

    @research_broker.task(task_name="research")
    async def research() -> str:
        return "research result"

    @review_broker.task(task_name="review")
    async def review() -> str:
        return "review result"

    await research_broker.startup()
    await review_broker.startup()
    try:
        research_task = await research.kicker().with_task_id("agent-run-42").kiq()
        review_task = await review.kicker().with_task_id("agent-run-42").kiq()
        tasks = [research_task, review_task]
        expected = ["research result", "review result"]
        if reverse:
            tasks.reverse()
            expected.reverse()

        results = await gather(*tasks, tasks[0], periodicity=0)

        assert [result.return_value for result in results] == [*expected, expected[0]]
    finally:
        await review_broker.shutdown()
        await research_broker.shutdown()


@pytest.mark.parametrize("ready_index", [0, 1])
async def test_gather_waits_for_same_id_in_each_backend(ready_index: int) -> None:
    """One ready branch must not make an unrelated pending branch look complete."""
    backends: list[InmemoryResultBackend[str]] = [
        InmemoryResultBackend(),
        InmemoryResultBackend(),
    ]
    await backends[ready_index].set_result(
        "agent-run-42",
        TaskiqResult(is_err=False, return_value="ready branch", execution_time=0),
    )
    tasks = [AsyncTaskiqTask("agent-run-42", backend) for backend in backends]

    with pytest.raises(TaskiqResultTimeoutError):
        await gather(*tasks, timeout=0.01, periodicity=0)


class ConsumingResultBackend(InmemoryResultBackend[Any]):
    """A result store that removes results when read, like keep_results=False."""

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[Any]:
        return self.results.pop(task_id)


async def test_gather_reads_duplicate_handles_from_one_backend_once() -> None:
    """Equivalent handles keep their positions without consuming a result twice."""
    backend = ConsumingResultBackend()
    await backend.set_result(
        "agent-run-42",
        TaskiqResult(is_err=False, return_value="shared result", execution_time=0),
    )
    first: AsyncTaskiqTask[Any] = AsyncTaskiqTask("agent-run-42", backend)
    equivalent: AsyncTaskiqTask[Any] = AsyncTaskiqTask("agent-run-42", backend)

    results = await gather(first, equivalent, first, periodicity=0)

    assert [result.return_value for result in results] == ["shared result"] * 3
    assert not await backend.is_result_ready("agent-run-42")


class ObservedResultBackend(InmemoryResultBackend[str]):
    """Expose the first poll so a worker can publish its result afterwards."""

    def __init__(self) -> None:
        super().__init__()
        self.polled = asyncio.Event()

    async def is_result_ready(self, task_id: str) -> bool:
        ready = await super().is_result_ready(task_id)
        self.polled.set()
        return ready


async def test_gather_collects_same_id_after_second_backend_becomes_ready() -> None:
    """A branch completing between polls contributes its own result."""
    first: InmemoryResultBackend[str] = InmemoryResultBackend()
    second = ObservedResultBackend()
    await first.set_result(
        "agent-run-42",
        TaskiqResult(is_err=False, return_value="first result", execution_time=0),
    )

    async def publish_second_result() -> None:
        await second.polled.wait()
        await second.set_result(
            "agent-run-42",
            TaskiqResult(is_err=False, return_value="second result", execution_time=0),
        )

    publisher = asyncio.create_task(publish_second_result())
    try:
        results = await asyncio.wait_for(
            gather(
                AsyncTaskiqTask("agent-run-42", first),
                AsyncTaskiqTask("agent-run-42", second),
                periodicity=0,
            ),
            timeout=1,
        )
        assert [result.return_value for result in results] == [
            "first result",
            "second result",
        ]
    finally:
        publisher.cancel()
        await asyncio.gather(publisher, return_exceptions=True)
