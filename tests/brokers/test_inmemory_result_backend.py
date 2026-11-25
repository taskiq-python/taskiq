import pytest

from taskiq.brokers.inmemory_broker import InmemoryResultBackend
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.result import TaskiqResult


@pytest.mark.parametrize("capacity", [1, 2, -1])
@pytest.mark.parametrize("update_first", [True, False])
async def test_result_updates_preserve_cached_tasks(
    capacity: int,
    *,
    update_first: bool,
) -> None:
    backend = InmemoryResultBackend[int](max_stored_results=capacity)
    task_ids = ["first"] if capacity == 1 else ["first", "second"]
    for task_id in task_ids:
        await backend.set_result(
            task_id,
            TaskiqResult(is_err=False, return_value=0, execution_time=0),
        )

    updated_id = task_ids[0] if update_first else task_ids[-1]
    for value in (1, 2):
        await backend.set_result(
            updated_id,
            TaskiqResult(is_err=False, return_value=value, execution_time=0),
        )
        for task_id in task_ids:
            assert await backend.is_result_ready(task_id)
        assert (await backend.get_result(updated_id)).return_value == value

    await backend.set_result(
        "new",
        TaskiqResult(is_err=False, return_value=3, execution_time=0),
    )
    assert await backend.is_result_ready("new")
    assert await backend.is_result_ready("first") is (capacity == -1)
    if capacity != 1:
        assert await backend.is_result_ready("second")


@pytest.mark.parametrize("capacity", [1, 2, -1])
@pytest.mark.parametrize("update_first", [True, False])
async def test_progress_updates_preserve_cached_tasks(
    capacity: int,
    *,
    update_first: bool,
) -> None:
    backend = InmemoryResultBackend[int](max_stored_results=capacity)
    task_ids = ["first"] if capacity == 1 else ["first", "second"]
    for task_id in task_ids:
        await backend.set_progress(
            task_id,
            TaskProgress(state="STARTED", meta=0),
        )

    updated_id = task_ids[0] if update_first else task_ids[-1]
    for value in (1, 2):
        await backend.set_progress(
            updated_id,
            TaskProgress(state="STARTED", meta=value),
        )
        for task_id in task_ids:
            assert await backend.get_progress(task_id) is not None
        progress = await backend.get_progress(updated_id)
        assert progress is not None
        assert progress.meta == value

    await backend.set_progress("new", TaskProgress(state="STARTED", meta=3))
    assert await backend.get_progress("new") is not None
    assert (await backend.get_progress("first") is not None) is (capacity == -1)
    if capacity != 1:
        assert await backend.get_progress("second") is not None
