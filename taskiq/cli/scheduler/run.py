import asyncio
import inspect
import sys
from datetime import datetime, timedelta, timezone
from logging import basicConfig, getLevelName, getLogger
from typing import Any, Dict, List, Optional, Set, Tuple

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
) -> List[Tuple[ScheduleSource, List[ScheduledTask]]]:
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
    return list(zip(scheduler.sources, schedules))


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
    logger.debug(
        "Waiting %d seconds before sending task %s with schedule_id %s.",
        delay,
        task.task_name,
        task.schedule_id,
    )
    if delay > 0:
        await asyncio.sleep(delay)
    logger.info(
        "Sending task %s with schedule_id %s.",
        task.task_name,
        task.schedule_id,
    )
    await scheduler.on_ready(source, task)


async def run_scheduler_loop(  # noqa: C901
    scheduler: TaskiqScheduler,
    interval: Optional[timedelta] = None,
) -> None:
    """
    Runs scheduler loop.

    This function imports taskiq scheduler
    and runs tasks when needed.

    :param scheduler: current scheduler.
    :param interval: interval to check for schedule updates.
    """
    loop = asyncio.get_event_loop()
    running_schedules: Dict[str, asyncio.Task[Any]] = {}
    ran_cron_jobs: Set[str] = set()
    current_minute = datetime.now(tz=pytz.UTC).minute
    while True:
        now = datetime.now(tz=pytz.UTC)
        # If minute changed, we need to clear
        # ran_cron_jobs set and update current minute.
        if now.minute != current_minute:
            current_minute = now.minute
            ran_cron_jobs.clear()
        # If interval is not None, we need to
        # calculate next run time using it.
        if interval is not None:
            next_run = now + interval
        # otherwise we need assume that
        # we will run it at the start of the next minute.
        # as crontab does.
        else:
            next_run = (now + timedelta(minutes=1)).replace(second=1, microsecond=0)
        scheduled_tasks = await get_all_schedules(scheduler)
        for source, task_list in scheduled_tasks:
            logger.debug("Got %d schedules from source %s.", len(task_list), source)
            for task in task_list:
                try:
                    task_delay = get_task_delay(task)
                except ValueError:
                    logger.warning(
                        "Cannot parse cron: %s for task: %s, schedule_id: %s.",
                        task.cron,
                        task.task_name,
                        task.schedule_id,
                    )
                    continue
                # If task delay is None, we don't need to run it.
                if task_delay is None:
                    continue
                # If task is delayed for more than next_run,
                # we don't need to run it, because we will
                # run it in the next iteration.
                if now + timedelta(seconds=task_delay) >= next_run:
                    continue
                # If task is already running, we don't need to run it again.
                if task.schedule_id in running_schedules and task_delay < 1:
                    continue
                # If task is cron job, we need to check if
                # we already ran it this minute.
                if task.cron is not None:
                    if task.schedule_id in ran_cron_jobs:
                        continue
                    ran_cron_jobs.add(task.schedule_id)
                send_task = loop.create_task(
                    delayed_send(scheduler, source, task, task_delay),
                    # We need to set the name of the task
                    # to be able to discard its reference
                    # after it is done.
                    name=f"schedule_{task.schedule_id}",
                )
                running_schedules[task.schedule_id] = send_task
                send_task.add_done_callback(
                    lambda task_future: running_schedules.pop(
                        task_future.get_name().removeprefix("schedule_"),
                    ),
                )
        delay = next_run - datetime.now(tz=pytz.UTC)
        logger.debug(
            "Sleeping for %.2f seconds before getting schedules.",
            delay.total_seconds(),
        )
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
        scheduler = import_object(args.scheduler, app_dir=args.app_dir)
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

    interval = None
    if args.update_interval:
        interval = timedelta(seconds=args.update_interval)

    logger.info("Starting scheduler.")
    await scheduler.startup()
    logger.info("Startup completed.")
    if args.skip_first_run:
        next_minute = datetime.now(timezone.utc).replace(
            second=0,
            microsecond=0,
        ) + timedelta(
            minutes=1,
        )
        delay = next_minute - datetime.now(timezone.utc)
        delay_secs = int(delay.total_seconds())
        logger.info(f"Skipping first run. Waiting {delay_secs} seconds.")
        await asyncio.sleep(delay.total_seconds())
        logger.info("First run skipped. The scheduler is now running.")
    try:
        await run_scheduler_loop(scheduler, interval)
    except asyncio.CancelledError:
        logger.warning("Shutting down scheduler.")
        await scheduler.shutdown()
        for source in scheduler.sources:
            await source.shutdown()
        logger.info("Scheduler shut down. Good bye!")
