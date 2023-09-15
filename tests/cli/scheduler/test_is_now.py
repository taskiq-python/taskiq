import datetime

import pytz
from tzlocal import get_localzone

from taskiq.cli.scheduler.run import should_run
from taskiq.schedule_sources.label_based import LabelScheduleSource
from taskiq.scheduler.scheduler import ScheduledTask

DUMMY_SOURCE = LabelScheduleSource(broker=None)  # type: ignore


def test_should_run_success() -> None:
    hour = datetime.datetime.utcnow().hour
    assert should_run(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            source=DUMMY_SOURCE,
            cron=f"* {hour} * * *",
        ),
    )


def test_should_run_cron_str_offset() -> None:
    hour = datetime.datetime.now().hour
    zone = get_localzone()
    assert should_run(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            source=DUMMY_SOURCE,
            cron=f"* {hour} * * *",
            cron_offset=str(zone),
        ),
    )


def test_should_run_cron_td_offset() -> None:
    offset = 2
    hour = datetime.datetime.utcnow().hour + offset
    assert should_run(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            source=DUMMY_SOURCE,
            cron=f"* {hour} * * *",
            cron_offset=datetime.timedelta(hours=offset),
        ),
    )


def test_time_utc_without_zone() -> None:
    time = datetime.datetime.utcnow()
    assert should_run(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            source=DUMMY_SOURCE,
            time=time - datetime.timedelta(seconds=1),
        ),
    )


def test_time_utc_with_zone() -> None:
    time = datetime.datetime.now(tz=pytz.UTC)
    assert should_run(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            source=DUMMY_SOURCE,
            time=time - datetime.timedelta(seconds=1),
        ),
    )


def test_time_utc_with_local_zone() -> None:
    localtz = get_localzone()
    time = datetime.datetime.now(tz=localtz)
    assert should_run(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            source=DUMMY_SOURCE,
            time=time - datetime.timedelta(seconds=1),
        ),
    )


def test_time_localtime_without_zone() -> None:
    time = datetime.datetime.now(tz=pytz.FixedOffset(240)).replace(tzinfo=None)
    assert not should_run(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            source=DUMMY_SOURCE,
            time=time - datetime.timedelta(seconds=1),
        ),
    )
