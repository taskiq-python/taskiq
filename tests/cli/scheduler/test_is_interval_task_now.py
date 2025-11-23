from datetime import datetime, timedelta

import pytest

from taskiq.cli.scheduler.run import is_interval_task_now


@pytest.mark.parametrize(
    "interval_value,now,last_run,expected",
    [
        (
            timedelta(seconds=10),
            datetime(2023, 1, 1, 0, 0, 15),
            datetime(2023, 1, 1, 0, 0, 0),
            True,
        ),
        (10, datetime(2023, 1, 1, 0, 0, 15), datetime(2023, 1, 1, 0, 0, 0), True),
        (
            timedelta(seconds=10),
            datetime(2023, 1, 1, 0, 0, 5),
            datetime(2023, 1, 1, 0, 0, 0),
            False,
        ),
        (timedelta(seconds=10), datetime(2023, 1, 1, 0, 0, 0), None, True),
        (1, datetime(2023, 1, 1, 0, 0, 2), datetime(2023, 1, 1, 0, 0, 0), True),
    ],
)
def test_is_interval_task_now(
    interval_value: int | timedelta,
    now: datetime,
    last_run: datetime | None,
    expected: bool,
) -> None:
    assert is_interval_task_now(interval_value, now, last_run) == expected
