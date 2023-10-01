from taskiq.cli.scheduler.run import run_scheduler_loop
from taskiq.scheduler import TaskiqScheduler


async def run_scheduler_task(
    scheduler: TaskiqScheduler,
    run_startup: bool = False,
) -> None:
    """
    Run scheduler task.

    This task runs scheduler loop and starts all sources.
    Use this function to run scheduler programmatically.

    :param scheduler: scheduler instance.
    :param run_startup: whether to run startup function or not.
    """
    for source in scheduler.sources:
        await source.startup()
    if run_startup:
        await scheduler.startup()
    while True:
        await run_scheduler_loop(scheduler)
