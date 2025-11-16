from datetime import timedelta
from typing import Union

import pytest

from taskiq.scheduler.scheduled_task.validators import validate_interval_value


@pytest.mark.parametrize(
    "value",
    [
        None,
        1,
        5,
        3600,
        timedelta(seconds=1),
        timedelta(seconds=5),
        timedelta(hours=1, minutes=30),
        timedelta(seconds=1, microseconds=0),
    ],
)
def test_validate_interval_value_success(value: Union[int, timedelta, None]) -> None:
    validate_interval_value(value)


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        timedelta(seconds=0),
        timedelta(milliseconds=999),
        timedelta(seconds=1.5),
        timedelta(seconds=0.999),
        timedelta(seconds=1, microseconds=1),
    ],
)
def test_validate_interval_value_fail(value: Union[int, timedelta, None]) -> None:
    with pytest.raises(ValueError):
        validate_interval_value(value)
