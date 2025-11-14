from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

from taskiq.cli.scheduler.run import is_time_task_now


def test_time_utc_without_zone() -> None:
    now = datetime.now(tz=timezone.utc)
    is_now = is_time_task_now(
        time_value=now - timedelta(seconds=1),
        now=now,
    )
    assert is_now


def test_time_with_utc_zone() -> None:
    now = datetime.now()
    is_now = is_time_task_now(
        time_value=now - timedelta(seconds=1),
        now=now.replace(tzinfo=timezone.utc),
    )
    assert is_now


def test_time_with_local_zone() -> None:
    now = datetime.now()
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
