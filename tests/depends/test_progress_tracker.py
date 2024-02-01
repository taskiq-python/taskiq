from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

import pytest
from pydantic import ValidationError

from taskiq import (
    AsyncTaskiqDecoratedTask,
    InMemoryBroker,
    TaskiqDepends,
    TaskiqMessage,
)
from taskiq.abc import AsyncBroker
from taskiq.depends.progress_tracker import ProgressTracker, TaskState
from taskiq.receiver import Receiver


def get_receiver(
    broker: Optional[AsyncBroker] = None,
    no_parse: bool = False,
    max_async_tasks: Optional[int] = None,
) -> Receiver:
    """
    Returns receiver with custom broker and args.

    :param broker: broker, defaults to None
    :param no_parse: parameter to taskiq_args, defaults to False
    :param cli_args: Taskiq worker CLI arguments.
    :return: new receiver.
    """
    if broker is None:
        broker = InMemoryBroker()
    return Receiver(
        broker,
        executor=ThreadPoolExecutor(max_workers=10),
        validate_params=not no_parse,
        max_async_tasks=max_async_tasks,
    )


def get_message(
    task: AsyncTaskiqDecoratedTask[Any, Any],
    task_id: Optional[str] = None,
    *args: Any,
    labels: Optional[Dict[str, str]] = None,
    **kwargs: Dict[str, Any],
) -> TaskiqMessage:
    if labels is None:
        labels = {}
    return TaskiqMessage(
        task_id=task_id or task.broker.id_generator(),
        task_name=task.task_name,
        labels=labels,
        args=list(args),
        kwargs=kwargs,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "state,meta",
    [
        (TaskState.STARTED, "hello world!"),
        ("retry", "retry error!"),
        ("custom state", {"Complex": "Value"}),
    ],
)
async def test_progress_tracker_ctx_raw(state: Any, meta: Any) -> None:
    broker = InMemoryBroker()

    @broker.task
    async def test_func(tes_val: ProgressTracker[Any] = TaskiqDepends()) -> None:
        await tes_val.set_progress(state, meta)

    kicker = await test_func.kiq()
    result = await kicker.wait_result()

    assert not result.is_err
    progress = await broker.result_backend.get_progress(kicker.task_id)
    assert progress is not None
    assert progress.meta == meta
    assert progress.state == state


@pytest.mark.anyio
async def test_progress_tracker_ctx_none() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def test_func() -> None:
        pass

    kicker = await test_func.kiq()
    result = await kicker.wait_result()

    assert not result.is_err
    progress = await broker.result_backend.get_progress(kicker.task_id)
    assert progress is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    "state,meta",
    [
        (("state", "error"), 1),
    ],
)
async def test_progress_tracker_validation_error(state: Any, meta: Any) -> None:
    broker = InMemoryBroker()

    @broker.task
    async def test_func(progress: ProgressTracker[int] = TaskiqDepends()) -> None:
        await progress.set_progress(state, meta)  # type: ignore

    kicker = await test_func.kiq()
    result = await kicker.wait_result()
    with pytest.raises(ValidationError):
        result.raise_for_error()

    progress = await broker.result_backend.get_progress(kicker.task_id)
    assert progress is None
