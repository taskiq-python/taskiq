import asyncio
import inspect
import sys
from datetime import datetime, timedelta
from logging import basicConfig, getLevelName, getLogger
from typing import Dict, List, Optional

import pytz
from pycron import is_now

from taskiq.abc.schedule_source import ScheduleSource
from taskiq.cli.scheduler.args import SchedulerArgs
from taskiq.cli.utils import import_object, import_tasks
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler

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


async def get_schedules(source: ScheduleSource) -> List[ScheduledTask]:
    """
    Get schedules from source.

    If source raises an exception, it will be
    logged and an empty list will be returned.

    :param source: source to get schedules from.
    """
    try:
        return await source.get_schedules()
    except Exception as exc:
        logger.warning(
            "Cannot update schedules with source: %s",
            source,
        )
        logger.debug(exc, exc_info=True)
        return []


async def get_all_schedules(
    scheduler: TaskiqScheduler,
) -> Dict[ScheduleSource, List[ScheduledTask]]:
    """
    Task to update all schedules.

    This function updates all schedules
    from all sources and returns a dict
    with source as a key and list of
    scheduled tasks as a value.

    :param scheduler: current scheduler.
    :return: dict with source as a key and list of scheduled tasks as a value.
    """
    logger.debug("Started schedule update.")
    schedules = await asyncio.gather(
        *[get_schedules(source) for source in scheduler.sources],
    )
    return dict(zip(scheduler.sources, schedules))


def get_task_delay(task: ScheduledTask) -> Optional[int]:
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
    if task.time is not None:
        task_time = to_tz_aware(task.time)
        if task_time <= now:
            return 0
        one_min_ahead = (now + timedelta(minutes=1)).replace(second=1, microsecond=0)
        if task_time <= one_min_ahead:
            delay = task_time - now
            if delay.microseconds:
                return int(delay.total_seconds()) + 1
            return int(delay.total_seconds())
    return None


async def delayed_send(
    scheduler: TaskiqScheduler,
    source: ScheduleSource,
    task: ScheduledTask,
    delay: int,
) -> None:
    """
    Send a task with a delay.

    This function waits for some time and then
    sends a task.

    The main idea is that scheduler gathers
    tasks every minute and some of them have
    specific time. To respect the time, we calculate
    the delay and send the task after some delay.

    :param scheduler: current scheduler.
    :param source: source of the task.
    :param task: task to send.
    :param delay: how long to wait.
    """
    if delay > 0:
        await asyncio.sleep(delay)
    logger.info("Sending task %s.", task.task_name)
    await scheduler.on_ready(source, task)


async def run_scheduler_loop(scheduler: TaskiqScheduler) -> None:
    """
    Runs scheduler loop.

    This function imports taskiq scheduler
    and runs tasks when needed.

    :param scheduler: current scheduler.
    """
    loop = asyncio.get_event_loop()
    running_schedules = set()
    while True:
        # We use this method to correctly sleep for one minute.
        scheduled_tasks = await get_all_schedules(scheduler)
        for source, task_list in scheduled_tasks.items():
            for task in task_list:
                try:
                    task_delay = get_task_delay(task)
                except ValueError:
                    logger.warning(
                        "Cannot parse cron: %s for task: %s, schedule_id: %s",
                        task.cron,
                        task.task_name,
                        task.schedule_id,
                    )
                    continue
                if task_delay is not None:
                    send_task = loop.create_task(
                        delayed_send(scheduler, source, task, task_delay),
                    )
                    running_schedules.add(send_task)
                    send_task.add_done_callback(running_schedules.discard)
        next_minute = datetime.now().replace(second=0, microsecond=0) + timedelta(
            minutes=1,
        )
        delay = next_minute - datetime.now()
        await asyncio.sleep(delay.total_seconds())


async def run_scheduler(args: SchedulerArgs) -> None:
    """
    Run scheduler.

    This function takes all CLI arguments
    and starts the scheduler process.

    :param args: parsed CLI arguments.
    """
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

    if isinstance(args.scheduler, str):
        scheduler = import_object(args.scheduler)
        if inspect.isfunction(scheduler):
            scheduler = scheduler()
    else:
        scheduler = args.scheduler
    if not isinstance(scheduler, TaskiqScheduler):
        logger.error(
            "Imported scheduler is not a subclass of TaskiqScheduler.",
        )
        sys.exit(1)

    scheduler.broker.is_scheduler_process = True
    import_tasks(args.modules, args.tasks_pattern, args.fs_discover)
    for source in scheduler.sources:
        await source.startup()

    logger.info("Starting scheduler.")
    await scheduler.startup()
    logger.info("Startup completed.")
    if args.skip_first_run:
        next_minute = datetime.utcnow().replace(second=0, microsecond=0) + timedelta(
            minutes=1,
        )
        delay = next_minute - datetime.utcnow()
        delay_secs = int(delay.total_seconds())
        logger.info(f"Skipping first run. Waiting {delay_secs} seconds.")
        await asyncio.sleep(delay.total_seconds())
        logger.info("First run skipped. The scheduler is now running.")
    try:
        await run_scheduler_loop(scheduler)
    except asyncio.CancelledError:
        logger.warning("Shutting down scheduler.")
        await scheduler.shutdown()
        for source in scheduler.sources:
            await source.shutdown()
        logger.info("Scheduler shut down. Good bye!")
