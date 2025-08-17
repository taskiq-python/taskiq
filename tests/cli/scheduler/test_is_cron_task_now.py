from datetime import datetime, timedelta, timezone
from typing import Optional, Union

import pytest
import pytz
from tzlocal import get_localzone

from taskiq.cli.scheduler.run import CronValueError, is_cron_task_now


def test_should_run_success() -> None:
    now = datetime.now(timezone.utc)
    is_now = is_cron_task_now(
        cron_value=f"* {now.hour} * * *",
        now=now,
    )
    assert is_now


def test_should_run_cron_str_offset() -> None:
    now = datetime.now(timezone.utc)
    hour = datetime.now().hour
    zone = get_localzone()
    is_now = is_cron_task_now(
        cron_value=f"* {hour} * * *",
        offset=str(zone),
        now=now,
    )
    assert is_now


def test_should_run_cron_td_offset() -> None:
    offset = 2
    now = datetime.now(timezone.utc)
    hour = (now.hour + offset) % 24
    is_now = is_cron_task_now(
        cron_value=f"* {hour} * * *",
        offset=timedelta(hours=offset),
        now=now,
    )
    assert is_now


@pytest.mark.parametrize(
    "cron_value,now,offset,last_run,expected",
    [
        ("* * * * *", datetime(2023, 1, 1, 0, 0, 30), None, None, True),
        ("* * * * *", datetime(2023, 1, 1, 0, 0, 30), timedelta(hours=1), None, True),
        ("* * * * *", datetime(2023, 1, 1, 0, 0, 30), "US/Eastern", None, True),
        (
            "* * * * *",
            datetime(2023, 1, 1, 0, 0, 30),
            None,
            datetime(2023, 1, 1, 0, 0, 0),
            False,
        ),
        (
            "* * * * *",
            datetime(2023, 1, 1, 0, 0, 30),
            None,
            datetime(2023, 1, 1, 0, 0, 29),
            False,
        ),
        ("0 * * * *", datetime(2023, 1, 1, 0, 30, 0), None, None, False),
    ],
)
def test_is_cron_task_now(
    cron_value: str,
    now: datetime,
    offset: Union[str, timedelta, None],
    last_run: Optional[datetime],
    expected: bool,
) -> None:
    now = pytz.UTC.localize(now)
    if last_run:
        last_run = pytz.UTC.localize(last_run)

    assert is_cron_task_now(cron_value, now, offset, last_run) == expected


def test_is_cron_task_now_invalid_cron() -> None:
    with pytest.raises(CronValueError):
        is_cron_task_now("invalid cron", datetime.now())
