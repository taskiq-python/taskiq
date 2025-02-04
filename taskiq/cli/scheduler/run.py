import asyncio
import sys
from datetime import datetime, timedelta
from logging import basicConfig, getLevelName, getLogger
from typing import Dict, List, Optional, Union

import pycron
import pytz

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


class _CronParseError(Exception):
    pass


def is_cron_now(
    value: str,
    offset: Union[str, timedelta, None] = None,
) -> bool:
    """Determines whether a cron-like string is valid for the current date and time.

    :param value: cron-like string.
    :param offset: cron offset.
    """
    now = datetime.now(tz=pytz.UTC)
    # If user specified cron offset we apply it.
    # If it's timedelta, we simply add the delta to current time.
    if offset and isinstance(offset, timedelta):
        now += offset
    # If timezone was specified as string we convert it timezone
    # offset and then apply.
    elif offset and isinstance(offset, str):
        now = now.astimezone(pytz.timezone(offset))

    try:
        return pycron.is_now(value, now)
    except ValueError as e:
        raise _CronParseError(f"Cannot parse cron: '{value}'") from e


def get_task_delay(task: ScheduledTask) -> Optional[int]:
    """
    Get delay of the task in seconds.

    :param task: task to check.
    :return: True if task must be sent.
    """
    now = datetime.now(tz=pytz.UTC)
    if task.cron is not None:
        return 0 if is_cron_now(task.cron, task.cron_offset) else None
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


async def send(
    scheduler: TaskiqScheduler,
    source: ScheduleSource,
    task: ScheduledTask,
) -> None:
    """
    Send a task.

    :param scheduler: current scheduler.
    :param source: source of the task.
    :param task: task to send.
    """
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

    interval_tasks_last_run: "Dict[str, datetime]" = {}
    now = datetime.now()
    next_minute = now.replace(second=0, microsecond=0)
    next_second = now.replace(microsecond=0) + timedelta(seconds=1)

    await asyncio.sleep((next_second - now).total_seconds())

    while True:
        # We use this method to correctly sleep.
        now = datetime.now()
        scheduled_tasks = await get_all_schedules(scheduler)
        for source, task_list in scheduled_tasks.items():
            for task in task_list:
                to_send = False
                if task.time is not None and now >= task.time:
                    to_send = True
                elif task.interval is not None:
                    last_call = interval_tasks_last_run.get(task.task_name)
                    if (
                        last_call is not None
                        and round((now - last_call).total_seconds()) < task.interval
                    ):
                        to_send = False
                    else:
                        to_send = True
                        interval_tasks_last_run[task.task_name] = now
                elif now > next_minute and task.cron is not None:
                    try:
                        to_send = is_cron_now(task.cron, task.cron_offset)
                    except _CronParseError:
                        logger.warning(
                            "Cannot parse cron: %s for task: %s, schedule_id: %s",
                            task.cron,
                            task.task_name,
                            task.schedule_id,
                        )

                if to_send:
                    send_task = loop.create_task(send(scheduler, source, task))
                    running_schedules.add(send_task)
                    send_task.add_done_callback(running_schedules.discard)

        now = datetime.now()
        next_second = now.replace(microsecond=0) + timedelta(seconds=1)
        next_minute = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        await asyncio.sleep((next_second - now).total_seconds())


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
