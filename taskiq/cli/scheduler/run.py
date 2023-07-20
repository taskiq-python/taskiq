import asyncio
from datetime import datetime, timedelta
from logging import basicConfig, getLevelName, getLogger
from typing import List

from pycron import is_now

from taskiq.cli.scheduler.args import SchedulerArgs
from taskiq.cli.utils import import_object, import_tasks
from taskiq.scheduler.scheduler import ScheduledTask, TaskiqScheduler

logger = getLogger(__name__)


async def schedules_updater(
    scheduler: TaskiqScheduler,
    current_schedules: List[ScheduledTask],
) -> None:
    """
    Periodic update to schedules.

    This task periodically checks for new schedules,
    assembles the final list and replaces current
    schedule with a new one.

    :param scheduler: current scheduler.
    :param current_schedules: list of schedules.
    """
    while True:
        logger.debug("Started schedule update.")
        new_schedules: "List[ScheduledTask]" = []
        for source in scheduler.sources:
            try:
                schedules = await source.get_schedules()
            except Exception as exc:
                logger.warning(
                    "Cannot update schedules with source: %s",
                    source,
                )
                logger.debug(exc, exc_info=True)
                continue

            for schedule in scheduler.merge_func(new_schedules, schedules):
                new_schedules.append(schedule)

        current_schedules.clear()
        current_schedules.extend(new_schedules)
        await asyncio.sleep(scheduler.refresh_delay)


def should_run(task: ScheduledTask) -> bool:
    """
    Checks if it's time to run a task.

    :param task: task to check.
    :return: True if task must be sent.
    """
    if task.cron is not None:
        return is_now(task.cron, datetime.utcnow())
    if task.time is not None:
        return task.time <= datetime.utcnow()
    return False


async def run_scheduler(args: SchedulerArgs) -> None:  # noqa: C901, WPS210, WPS213
    """
    Runs scheduler loop.

    This function imports taskiq scheduler
    and runs tasks when needed.

    :param args: parsed CLI args.
    """
    if isinstance(args.scheduler, str):
        scheduler = import_object(args.scheduler)
    else:
        scheduler = args.scheduler
    if not isinstance(scheduler, TaskiqScheduler):
        print(  # noqa: WPS421
            "Imported scheduler is not a subclass of TaskiqScheduler.",
        )
        exit(1)  # noqa: WPS421
    scheduler.broker.is_scheduler_process = True
    import_tasks(args.modules, args.tasks_pattern, args.fs_discover)
    if args.configure_logging:
        basicConfig(
            level=getLevelName(args.log_level),
            format=(
                "[%(asctime)s][%(levelname)-7s]"
                "[%(module)s:%(funcName)s:%(lineno)d]"
                " %(message)s"
            ),
        )
    getLogger("taskiq").setLevel(level=getLevelName(args.log_level))
    for source in scheduler.sources:
        await source.startup()
    loop = asyncio.get_event_loop()
    tasks: "List[ScheduledTask]" = []
    loop.create_task(schedules_updater(scheduler, tasks))
    logger.info("Starting scheduler.")
    await scheduler.startup()
    logger.info("Startup completed.")
    while True:  # noqa: WPS457
        for task in tasks:
            try:
                ready = should_run(task)
            except ValueError:
                logger.warning(
                    "Cannot parse cron: %s for task: %s",
                    task.cron,
                    task.task_name,
                )
                continue
            if ready:
                logger.info("Sending task %s.", task.task_name)
                loop.create_task(scheduler.on_ready(task))

        delay = (
            datetime.now().replace(second=1, microsecond=0) + timedelta(minutes=1) - datetime.now()
        )
        await asyncio.sleep(delay.total_seconds())
