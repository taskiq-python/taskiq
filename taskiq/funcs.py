import asyncio
from time import time
from typing import Any

from taskiq.exceptions import TaskiqResultTimeoutError
from taskiq.result import TaskiqResult
from taskiq.task import AsyncTaskiqTask


async def gather(
    *tasks: AsyncTaskiqTask[Any],
    timeout: float = -1,
    with_logs: bool = False,
    periodicity: float = 0.1,
) -> tuple[TaskiqResult[Any], ...]:
    """
    Function to await results of several tasks.

    This function will wait for results of every
    specified task.

    This function returns tuple of results in the same
    order they were passed.

    Tasks with the same ID in different result backend instances
    are awaited independently. Repeated references to the same backend
    instance and task ID reuse the fetched result.

    :param tasks: tasks to await.
    :param timeout: timeout of awaiting, defaults to -1
    :param with_logs: whether you want to fetch logs, defaults to False
    :param periodicity: how often to ask for results, defaults to 0.1
    :raises TaskiqResultTimeoutError: if timeout is reached.
    :return: list of TaskiqResults.
    """
    loop = asyncio.get_event_loop()
    start_time = time()
    # A task ID identifies a result only within its backend.
    ordered_keys = [(id(task.result_backend), task.task_id) for task in tasks]
    task_keys = set(ordered_keys)
    results = {}

    async def check_task(task: AsyncTaskiqTask[Any]) -> None:
        nonlocal results
        nonlocal task_keys
        task_key = (id(task.result_backend), task.task_id)
        if await task.is_ready():
            try:
                task_keys.remove(task_key)
            except LookupError:
                return
            results[task_key] = await task.get_result(with_logs=with_logs)

    while task_keys:
        if 0 < timeout < time() - start_time:
            raise TaskiqResultTimeoutError(timeout=timeout)
        check_tasks = []
        for task in tasks:
            check_tasks.append(loop.create_task(check_task(task)))
        await asyncio.gather(*check_tasks)
        await asyncio.sleep(periodicity)

    return tuple(results[task_key] for task_key in ordered_keys)
