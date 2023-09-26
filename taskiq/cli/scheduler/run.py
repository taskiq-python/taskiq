import asyncio
from datetime import datetime, timedelta
from logging import basicConfig, getLevelName, getLogger
from typing import List, Optional

import pytz
from pycron import is_now

from taskiq.cli.scheduler.args import SchedulerArgs
from taskiq.cli.utils import import_object, import_tasks
from taskiq.scheduler.scheduler import ScheduledTask, TaskiqScheduler

logger = getLogger(__name__)


def to_tz_aware(time: datetime) -> datetime:
    """
    Convert datetime to timezone aware.

    This function takes a datetime and if
    timezone was not yet specified, it will
    be set to UTC.

    :param time: time to make timezone aware.
    :return: timezone aware time.
    """
    if time.tzinfo is None:
        return time.replace(tzinfo=pytz.UTC)
    return time


async def schedules_updater(
    scheduler: TaskiqScheduler,
    current_schedules: List[ScheduledTask],
    event: asyncio.Event,
) -> None:
    """
    Periodic update to schedules.

    This task periodically checks for new schedules,
    assembles the final list and replaces current
    schedule with a new one.

    :param scheduler: current scheduler.
    :param current_schedules: list of schedules.
    :param event: event when schedules are updated.
    """
    while True:  # noqa: WPS457
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

            new_schedules = scheduler.merge_func(new_schedules, schedules)

        current_schedules.clear()
        current_schedules.extend(new_schedules)
        event.set()
        await asyncio.sleep(scheduler.refresh_delay)


def get_task_delay(task: ScheduledTask) -> Optional[int]:  # noqa: C901
    """
    Get delay of the task in seconds.

    :param task: task to check.
    :return: True if task must be sent.
    """
    now = datetime.now(tz=pytz.UTC)
    if task.cron is not None:
        # If user specified cron offset we apply it.
        # If it's timedelta, we simply add the delta to current time.
        if task.cron_offset and isinstance(task.cron_offset, timedelta):
            now += task.cron_offset
        # If timezone was specified as string we convert it timzone
        # offset and then apply.
        elif task.cron_offset and isinstance(task.cron_offset, str):
            now = now.astimezone(pytz.timezone(task.cron_offset))
        if is_now(task.cron, now):
            return 0
        return None
    elif task.time is not None:
        task_time = to_tz_aware(task.time).replace(microsecond=0)
        if task_time <= now:
            return 0
        one_min_ahead = (now + timedelta(minutes=1)).replace(second=1, microsecond=0)
        if task_time <= one_min_ahead:
            return int((task_time - now).total_seconds())
    return None


async def delayed_send(
    scheduler: TaskiqScheduler,
    task: ScheduledTask,
    delay: int,
) -> None:
    """
    Send a task with a delay.

    This function waits for some time and then
    sends a task.

    The main idea is that scheduler gathers
    tasks every minute and some of them have
    specfic time. To respect the time, we calculate
    the delay and send the task after some delay.

    :param scheduler: current scheduler.
    :param task: task to send.
    :param delay: how long to wait.
    """
    if delay > 0:
        await asyncio.sleep(delay)
    logger.info("Sending task %s.", task.task_name)
    await scheduler.on_ready(task)


async def run_scheduler_loop(scheduler: TaskiqScheduler) -> None:  # noqa: WPS210
    """
    Runs scheduler loop.

    This function imports taskiq scheduler
    and runs tasks when needed.

    :param scheduler: current scheduler.
    """
    loop = asyncio.get_event_loop()
    tasks: "List[ScheduledTask]" = []

    current_task = asyncio.current_task()
    first_update_event = asyncio.Event()
    updater_task = loop.create_task(
        schedules_updater(
            scheduler,
            tasks,
            first_update_event,
        ),
    )
    if current_task is not None:
        current_task.add_done_callback(lambda _: updater_task.cancel())
    await first_update_event.wait()
    while True:  # noqa: WPS457
        for task in tasks:
            try:
                task_delay = get_task_delay(task)
            except ValueError:
                logger.warning(
                    "Cannot parse cron: %s for task: %s",
                    task.cron,
                    task.task_name,
                )
                continue
            if task_delay is not None:
                loop.create_task(delayed_send(scheduler, task, task_delay))

        delay = (
            datetime.now().replace(second=1, microsecond=0)
            + timedelta(minutes=1)
            - datetime.now()
        )
        await asyncio.sleep(delay.total_seconds())


async def run_scheduler(args: SchedulerArgs) -> None:  # noqa: WPS213
    """
    Run scheduler.

    This function takes all CLI arguments
    and starts the scheduler process.

    :param args: parsed CLI arguments.
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

    logger.info("Starting scheduler.")
    await scheduler.startup()
    logger.info("Startup completed.")

    try:
        await run_scheduler_loop(scheduler)
    except asyncio.CancelledError:
        logger.warning("Shutting down scheduler.")
        await scheduler.shutdown()
        logger.info("Scheduler shut down. Good bye!")
