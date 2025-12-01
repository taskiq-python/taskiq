from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from taskiq.cli.scheduler.run import to_tz_aware


@pytest.mark.parametrize(
    "input_time,expected_tz",
    [
        (datetime(2023, 1, 1), timezone.utc),
        (
            datetime(2023, 1, 1, tzinfo=ZoneInfo("US/Eastern")),
            ZoneInfo("US/Eastern"),
        ),
    ],
)
def test_to_tz_aware(
    input_time: datetime,
    expected_tz: ZoneInfo,
) -> None:
    result = to_tz_aware(input_time)
    assert result.tzinfo == expected_tz
