import asyncio
from time import time
from typing import Any, Tuple

from taskiq.exceptions import TaskiqResultTimeoutError
from taskiq.result import TaskiqResult
from taskiq.task import AsyncTaskiqTask


async def gather(
    *tasks: AsyncTaskiqTask[Any],
    timeout: float = -1,
    with_logs: bool = False,
    periodicity: float = 0.1,
) -> Tuple[TaskiqResult[Any], ...]:
    """
    Function to await results of several tasks.

    This function will wait for results of every
    specified task.

    This function returns tuple of results in the same
    order they were passed.

    :param tasks: tasks to await.
    :param timeout: timeout of awaiting, defaults to -1
    :param with_logs: wether you want to fetch logs, defaults to False
    :param periodicity: how often to ask for results, defaults to 0.1
    :raises TaskiqResultTimeoutError: if timeout is reached.
    :return: list of TaskiqResults.
    """
    loop = asyncio.get_event_loop()
    start_time = time()
    ordered_ids = [task.task_id for task in tasks]
    task_ids = {task.task_id for task in tasks}
    results = {}

    async def check_task(task: AsyncTaskiqTask[Any]) -> None:
        nonlocal results
        nonlocal task_ids
        if await task.is_ready():
            try:
                task_ids.remove(task.task_id)
            except LookupError:
                return
            results[task.task_id] = await task.get_result(with_logs=with_logs)

    while task_ids:
        if 0 < timeout < time() - start_time:
            raise TaskiqResultTimeoutError("Timed out")
        check_tasks = []
        for task in tasks:
            check_tasks.append(loop.create_task(check_task(task)))
        await asyncio.gather(*check_tasks)
        await asyncio.sleep(periodicity)

    return tuple(results[task_id] for task_id in ordered_ids)
