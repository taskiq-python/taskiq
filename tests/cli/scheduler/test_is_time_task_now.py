from datetime import datetime, timedelta
from typing import Optional

import pytest
import pytz
from tzlocal import get_localzone

from taskiq.cli.scheduler.run import is_time_task_now


def test_time_utc_without_zone() -> None:
    now = datetime.now()
    is_now = is_time_task_now(
        time_value=now - timedelta(seconds=1),
        now=now,
    )
    assert is_now


def test_time_utc_with_zone() -> None:
    now = datetime.now(tz=pytz.UTC)
    is_now = is_time_task_now(
        time_value=now - timedelta(seconds=1),
        now=now,
    )
    assert is_now


def test_time_utc_with_local_zone() -> None:
    localtz = get_localzone()
    now = datetime.now(tz=localtz)
    is_now = is_time_task_now(
        time_value=now - timedelta(seconds=1),
        now=now,
    )
    assert is_now


@pytest.mark.parametrize(
    "time_value,now,last_run,expected",
    [
        (datetime(2023, 1, 1), datetime(2023, 1, 2), None, True),
        (datetime(2023, 1, 1), datetime(2023, 1, 1), None, True),
        (datetime(2023, 1, 2), datetime(2023, 1, 1), None, False),
        (datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 1), False),
    ],
)
def test_is_time_task_now(
    time_value: datetime,
    now: datetime,
    last_run: Optional[datetime],
    expected: bool,
) -> None:
    assert is_time_task_now(time_value, now, last_run) == expected
