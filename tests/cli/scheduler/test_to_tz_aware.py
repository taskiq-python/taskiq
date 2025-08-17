from datetime import datetime

import pytest
import pytz

from taskiq.cli.scheduler.run import to_tz_aware


@pytest.mark.parametrize(
    "input_time,expected_tz",
    [
        (datetime(2023, 1, 1), pytz.UTC),
        (
            datetime(2023, 1, 1, tzinfo=pytz.timezone("US/Eastern")),
            pytz.timezone("US/Eastern"),
        ),
    ],
)
def test_to_tz_aware(
    input_time: datetime,
    expected_tz: pytz.BaseTzInfo,
) -> None:
    result = to_tz_aware(input_time)
    assert result.tzinfo == expected_tz
