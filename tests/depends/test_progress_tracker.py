from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

import pytest

from taskiq import (
    AsyncTaskiqDecoratedTask,
    InMemoryBroker,
    ProgressTracker,
    TaskiqDepends,
    TaskiqMessage,
    TaskProgress,
    TaskState,
)
from taskiq.abc import AsyncBroker
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
    labels: Dict[str, str] = {},
    **kwargs: Dict[str, Any],
) -> TaskiqMessage:
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

    receiver = get_receiver(broker)
    result = await receiver.run_task(test_func, get_message(test_func, "task_id"))

    assert not result.is_err
    progress = await broker.result_backend.get_progress("task_id")
    assert progress is not None
    assert progress.meta == meta
    assert progress.state == state


@pytest.mark.anyio
@pytest.mark.parametrize(
    "state,meta",
    [
        (TaskState.STARTED, "hello world!"),
        ("retry", "retry error!"),
        ("custom state", {"Complex": "Value"}),
    ],
)
async def test_progress_tracker_ctx_cls(state: Any, meta: Any) -> None:
    broker = InMemoryBroker()

    @broker.task
    async def test_func(tes_val: ProgressTracker[Any] = TaskiqDepends()) -> None:
        await tes_val.set_progress(task_progress)

    task_progress = TaskProgress(state=state, meta=meta)
    receiver = get_receiver(broker)
    result = await receiver.run_task(test_func, get_message(test_func, "task_id"))

    assert not result.is_err
    progress = await broker.result_backend.get_progress("task_id")
    assert progress is not None
    assert progress.meta == meta
    assert progress.state == state


@pytest.mark.anyio
async def test_progress_tracker_ctx_none() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def test_func() -> None:
        pass

    receiver = get_receiver(broker)
    result = await receiver.run_task(test_func, get_message(test_func, "task_id"))

    assert not result.is_err
    progress = await broker.result_backend.get_progress("task_id")
    assert progress is None
