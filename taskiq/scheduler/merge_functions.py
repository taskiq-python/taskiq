from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from taskiq.scheduler.scheduler import ScheduledTask


def preserve_all(
    old_tasks: List["ScheduledTask"],
    new_tasks: List["ScheduledTask"],
) -> List["ScheduledTask"]:
    """
    This function simply merges two lists.

    It adds new tasks to others.

    :param old_tasks: previously discovered tasks.
    :param new_tasks: newly discovered tasks.
    :return: merged list.
    """
    return old_tasks + new_tasks


def only_unique(
    old_tasks: List["ScheduledTask"],
    new_tasks: List["ScheduledTask"],
) -> List["ScheduledTask"]:
    """
    This function preserves only unique schedules.

    It checks every task and if the schedule is already
    in list, it won't be added.

    :param old_tasks: previously discovered tasks.
    :param new_tasks: newly discovered tasks.
    :return: list of unique schedules.
    """
    new_tasks = []
    new_tasks.extend(old_tasks)
    for task in new_tasks:
        if task not in new_tasks:
            new_tasks.append(task)
    return new_tasks
