import datetime
from zoneinfo import ZoneInfo

from freezegun import freeze_time

from taskiq.cli.scheduler.run import get_task_delay
from taskiq.scheduler.scheduled_task import ScheduledTask


def test_should_run_success() -> None:
    hour = datetime.datetime.now(datetime.timezone.utc).hour
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
    timezone = ZoneInfo("Europe/Paris")
    hour = datetime.datetime.now(tz=timezone).hour
    delay = get_task_delay(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            cron=f"* {hour} * * *",
            cron_offset=str(timezone),
        ),
    )
    assert delay is not None and delay >= 0


def test_should_run_cron_td_offset() -> None:
    offset = 2
    hour = (datetime.datetime.now(datetime.timezone.utc).hour + offset) % 24
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
    time = datetime.datetime.now()
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
    time = datetime.datetime.now(tz=datetime.timezone.utc)
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
    localtz = ZoneInfo("Europe/Paris")
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


@freeze_time("2023-01-14 12:00:00")
def test_time_localtime_without_zone() -> None:
    time = datetime.datetime.now(
        tz=datetime.timezone(datetime.timedelta(minutes=240)),
    ).replace(tzinfo=None)
    time_to_run = time - datetime.timedelta(seconds=1)

    delay = get_task_delay(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            time=time_to_run,
        ),
    )

    expected_delay = time_to_run.replace(
        tzinfo=datetime.timezone.utc,
    ) - datetime.datetime.now(
        datetime.timezone.utc,
    )

    assert delay == int(expected_delay.total_seconds())


@freeze_time("2023-01-14 12:00:00")
def test_time_delay() -> None:
    time = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(
        seconds=15,
    )
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


@freeze_time("2023-01-14 12:00:00.05")
def test_time_delay_with_milliseconds() -> None:
    time = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(
        seconds=15,
        milliseconds=150,
    )
    delay = get_task_delay(
        ScheduledTask(
            task_name="",
            labels={},
            args=[],
            kwargs={},
            time=time,
        ),
    )
    assert delay is not None and delay == 16
