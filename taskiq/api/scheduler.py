from datetime import timedelta

from taskiq.cli.scheduler.run import SchedulerLoop
from taskiq.scheduler.scheduler import TaskiqScheduler


async def run_scheduler_task(
    scheduler: TaskiqScheduler,
    run_startup: bool = False,
    interval: timedelta | None = None,
    loop_interval: timedelta | None = None,
) -> None:
    """
    Run scheduler task.

    This task runs scheduler loop and starts all sources.
    Use this function to run scheduler programmatically.

    :param scheduler: scheduler instance.
    :param run_startup: whether to run startup function or not.
    :param interval: interval to check for schedule updates.
    :param loop_interval: interval to check tasks to send.
    """
    for source in scheduler.sources:
        await source.startup()
    if run_startup:
        await scheduler.startup()
    while True:
        scheduler_loop = SchedulerLoop(scheduler)
        await scheduler_loop.run(
            update_interval=interval,
            loop_interval=loop_interval,
        )
