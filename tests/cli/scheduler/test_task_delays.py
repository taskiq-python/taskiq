import datetime

import pytz
from freezegun import freeze_time
from tzlocal import get_localzone

from taskiq.cli.scheduler.run import get_task_delay
from taskiq.scheduler.scheduler import ScheduledTask


def test_should_run_success() -> None:
    hour = datetime.datetime.utcnow().hour
    delay = get_task_delay(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            cron=f"* {hour} * * *",
        ),
    )
    assert delay is not None and delay >= 0


def test_should_run_cron_str_offset() -> None:
    hour = datetime.datetime.now().hour
    zone = get_localzone()
    delay = get_task_delay(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            cron=f"* {hour} * * *",
            cron_offset=str(zone),
        ),
    )
    assert delay is not None and delay >= 0


def test_should_run_cron_td_offset() -> None:
    offset = 2
    hour = (datetime.datetime.utcnow().hour + offset) % 24
    delay = get_task_delay(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            cron=f"* {hour} * * *",
            cron_offset=datetime.timedelta(hours=offset),
        ),
    )
    assert delay is not None and delay >= 0


def test_time_utc_without_zone() -> None:
    time = datetime.datetime.utcnow()
    delay = get_task_delay(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            time=time - datetime.timedelta(seconds=1),
        ),
    )
    assert delay is not None and delay >= 0


def test_time_utc_with_zone() -> None:
    time = datetime.datetime.now(tz=pytz.UTC)
    delay = get_task_delay(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            time=time - datetime.timedelta(seconds=1),
        ),
    )
    assert delay is not None and delay >= 0


def test_time_utc_with_local_zone() -> None:
    localtz = get_localzone()
    time = datetime.datetime.now(tz=localtz)
    delay = get_task_delay(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            time=time - datetime.timedelta(seconds=1),
        ),
    )
    assert delay is not None and delay >= 0


def test_time_localtime_without_zone() -> None:
    time = datetime.datetime.now(tz=pytz.FixedOffset(240)).replace(tzinfo=None)
    delay = get_task_delay(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            time=time - datetime.timedelta(seconds=1),
        ),
    )
    assert delay is None


@freeze_time("2023-01-14 12:00:00")
def test_time_delay() -> None:
    time = datetime.datetime.now(tz=pytz.UTC) + datetime.timedelta(seconds=15)
    delay = get_task_delay(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            time=time,
        ),
    )
    assert delay is not None and delay == 15
