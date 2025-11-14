import asyncio
import inspect
import sys
from datetime import datetime, timedelta, timezone
from logging import basicConfig, getLogger
from typing import Any, Dict, List, Optional, Union
from zoneinfo import ZoneInfo

import pycron
from typing_extensions import TypeAlias

from taskiq.abc.schedule_source import ScheduleSource
from taskiq.cli.scheduler.args import SchedulerArgs
from taskiq.cli.utils import import_object, import_tasks
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler

logger = getLogger(__name__)


ScheduleId: TypeAlias = str


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
        return time.replace(tzinfo=timezone.utc)
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
    schedules: List[List[ScheduledTask]] = await asyncio.gather(
        *[get_schedules(source) for source in scheduler.sources],
    )
    return dict(zip(scheduler.sources, schedules))


class CronValueError(Exception):
    """Raised on invalid cron value."""


def is_cron_task_now(
    cron_value: str,
    now: datetime,
    offset: Union[str, timedelta, None] = None,
    last_run: Optional[datetime] = None,
) -> bool:
    """
    Checks whether the cron task should start now.

    :raises CronValueError: On invalid cron value.
    """
    if last_run is not None:
        seconds_spend = (now - last_run).total_seconds()
        seconds_should_sped = timedelta(minutes=1).total_seconds()
        if round(seconds_spend) < round(seconds_should_sped):
            return False
    # If user specified cron offset we apply it.
    # If it's timedelta, we simply add the delta to current time.
    if offset and isinstance(offset, timedelta):
        now += offset
    # If timezone was specified as string we convert it timezone
    # offset and then apply.
    elif offset and isinstance(offset, str):
        now = now.astimezone(ZoneInfo(offset))

    try:
        return pycron.is_now(cron_value, now)
    except ValueError as e:
        raise CronValueError(e) from e


def is_time_task_now(
    time_value: datetime,
    now: datetime,
    last_run: Optional[datetime] = None,
) -> bool:
    """Checks whether the time task should start now."""
    if last_run is not None:
        return False

    time_value = to_tz_aware(time_value)
    now = to_tz_aware(now)
    return time_value <= now


def is_interval_task_now(
    interval_value: Union[int, timedelta],
    now: datetime,
    last_run: Optional[datetime] = None,
) -> bool:
    """
    Checks whether the interval task should start now.

    Interval tasks must have a minimum interval of 1 second.
    Fractional intervals (e.g., 0.5 seconds) are not supported.

    :param interval_value: Interval as int (seconds) or timedelta
    :param now: Current datetime
    :param last_run: Last run datetime, None for first run
    :return: True if task should run now
    """
    if last_run is None:
        return True

    if isinstance(interval_value, int):
        interval_value = timedelta(seconds=interval_value)

    seconds_passed: float = (now - last_run).total_seconds()
    interval_seconds: float = interval_value.total_seconds()

    return round(seconds_passed) >= round(interval_seconds)


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
    logger.info(
        "Sending task %s with schedule_id %s.",
        task.task_name,
        task.schedule_id,
    )
    await scheduler.on_ready(source, task)


async def _sleep_until_next_second() -> None:
    now = datetime.now(tz=timezone.utc)
    await asyncio.sleep(1 - now.microsecond / 1_000_000)


class SchedulerLoop:
    """Abstraction over scheduler loop."""

    def __init__(
        self,
        scheduler: TaskiqScheduler,
        *,
        event_loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.scheduler = scheduler
        self._event_loop = event_loop or asyncio.get_event_loop()

        # Variables for control the last run of schedules.
        self.cron_tasks_last_run: dict[ScheduleId, datetime] = {}
        self.interval_tasks_last_run: dict[ScheduleId, datetime] = {}
        self.time_tasks_last_run: dict[ScheduleId, datetime] = {}

        self.scheduled_tasks: Dict[ScheduleSource, List[ScheduledTask]] = {}
        self.scheduled_tasks_updated_at: Optional[datetime] = None
        self._update_schedules_task_future: Optional[asyncio.Task[Any]] = None

    def _update_schedules_task_future_callback(self, task_: asyncio.Task[Any]) -> None:
        self.scheduled_tasks = task_.result()

        new_schedules_ids: set[ScheduleId] = set()
        for source, task_list in self.scheduled_tasks.items():
            logger.debug("Got %d schedules from source %s.", len(task_list), source)
            new_schedules_ids.update({t.schedule_id for t in task_list})

        # Deleting irrelevant scheduled tasks so they don't take up memory.
        for id_ in self.cron_tasks_last_run.keys() - new_schedules_ids:
            del self.cron_tasks_last_run[id_]
        for id_ in self.interval_tasks_last_run.keys() - new_schedules_ids:
            del self.interval_tasks_last_run[id_]
        for id_ in self.time_tasks_last_run.keys() - new_schedules_ids:
            del self.time_tasks_last_run[id_]

    async def _update_scheduled_tasks(self) -> None:
        if (
            self._update_schedules_task_future is not None
            and not self._update_schedules_task_future.done()
        ):
            logger.warning(
                "Schedules getting task started "
                "before the previous one finished. "
                "Consider increasing the update_interval.",
            )
        else:
            self._update_schedules_task_future = self._event_loop.create_task(
                get_all_schedules(self.scheduler),
            )
            self._update_schedules_task_future.add_done_callback(
                self._update_schedules_task_future_callback,
            )

    def _mark_cron_tasks_as_already_run(self) -> None:
        current_minute = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
        for _, task_list in self.scheduled_tasks.items():
            for task in task_list:
                if task.cron is not None:
                    self.cron_tasks_last_run[task.schedule_id] = current_minute

    def _is_schedule_ready_to_send(
        self,
        task: ScheduledTask,
        now: datetime,
    ) -> bool:
        is_ready_to_send: bool = False

        if not is_ready_to_send and task.cron is not None:
            try:
                is_ready_to_send = is_cron_task_now(
                    cron_value=task.cron,
                    now=now,
                    offset=task.cron_offset,
                    last_run=self.cron_tasks_last_run.get(task.schedule_id),
                )
            except CronValueError:
                logger.warning(
                    "Cannot parse cron: %s for task: %s, schedule_id: %s.",
                    task.cron,
                    task.task_name,
                    task.schedule_id,
                )
            if is_ready_to_send:
                self.cron_tasks_last_run[task.schedule_id] = now

        if not is_ready_to_send and task.interval is not None:
            is_ready_to_send = is_interval_task_now(
                interval_value=task.interval,
                now=now,
                last_run=self.interval_tasks_last_run.get(task.schedule_id),
            )
            if is_ready_to_send:
                self.interval_tasks_last_run[task.schedule_id] = now

        if not is_ready_to_send and task.time is not None:
            is_ready_to_send = is_time_task_now(
                time_value=task.time,
                now=now,
                last_run=self.time_tasks_last_run.get(task.schedule_id),
            )
            if is_ready_to_send:
                self.time_tasks_last_run[task.schedule_id] = now

        return is_ready_to_send

    async def run(
        self,
        *,
        update_interval: Optional[timedelta] = None,
        loop_interval: Optional[timedelta] = None,
        skip_first_run: bool = False,
    ) -> None:
        """
        Runs scheduler loop.

        This function imports taskiq scheduler
        and runs tasks when needed.

        :param update_interval: interval to check for schedule updates.
        :param loop_interval: interval to check tasks to send.
        :param skip_first_run: Wait for the beginning of the next minute
            to skip the first run.
        """
        if update_interval is None:
            update_interval = timedelta(minutes=1)
        if loop_interval is None:
            loop_interval = timedelta(seconds=1)

        running_schedules: Dict[ScheduleId, asyncio.Task[Any]] = {}

        self.scheduled_tasks = await get_all_schedules(self.scheduler)
        self.scheduled_tasks_updated_at = datetime.now(tz=timezone.utc)

        if skip_first_run:
            self._mark_cron_tasks_as_already_run()

        await _sleep_until_next_second()

        while True:
            now = datetime.now(tz=timezone.utc)
            next_run = (now + loop_interval).replace(microsecond=0)

            if now - self.scheduled_tasks_updated_at >= update_interval:
                await self._update_scheduled_tasks()
                self.scheduled_tasks_updated_at = now

            for source, task_list in self.scheduled_tasks.items():
                for task in task_list:
                    is_ready_to_send: bool = self._is_schedule_ready_to_send(
                        task=task,
                        now=now,
                    )

                    if is_ready_to_send and task.schedule_id not in running_schedules:
                        send_task = self._event_loop.create_task(
                            send(self.scheduler, source, task),
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

            delay = next_run - datetime.now(tz=timezone.utc)
            logger.debug(
                "Sleeping for %.3f seconds before getting schedules.",
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
            level=args.log_level,
            format=(
                "[%(asctime)s][%(levelname)-7s]"
                "[%(module)s:%(funcName)s:%(lineno)d]"
                " %(message)s"
            ),
        )
    getLogger("taskiq").setLevel(level=args.log_level)

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

    update_interval = timedelta(seconds=60)
    if args.update_interval is not None:
        update_interval = timedelta(seconds=args.update_interval)

    loop_interval = timedelta(seconds=1)
    if args.loop_interval is not None:
        loop_interval = timedelta(seconds=args.loop_interval)

    logger.info("Starting scheduler.")
    await scheduler.startup()
    logger.info("Startup completed.")

    scheduler_loop = SchedulerLoop(scheduler)
    try:
        await scheduler_loop.run(
            update_interval=update_interval,
            loop_interval=loop_interval,
            skip_first_run=args.skip_first_run,
        )
    except asyncio.CancelledError:
        logger.warning("Shutting down scheduler.")
        await scheduler.shutdown()
        for source in scheduler.sources:
            await source.shutdown()
        logger.info("Scheduler shut down. Good bye!")
