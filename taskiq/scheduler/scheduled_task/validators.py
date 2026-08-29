from datetime import timedelta
from typing import Any

from pydantic import ValidationError

from taskiq.compat import parse_obj_as


def parse_cron_offset(value: Any) -> Any:
    """Restore a ``timedelta`` cron offset from its serialized form.

    pydantic serializes a ``timedelta`` offset to an ISO-8601 duration
    string (e.g. ``"PT4H"``). Since the offset field is typed as
    ``str | timedelta``, on reload the value stays a ``str``, which then
    breaks timezone handling in the scheduler (``ZoneInfo("PT4H")``).

    Parse such duration strings back to ``timedelta`` while leaving genuine
    timezone names (e.g. ``"US/Eastern"``) and other values untouched.

    :param value: raw offset value coming from validation.
    :return: a ``timedelta`` for serialized durations, otherwise ``value``.
    """
    if not isinstance(value, str):
        return value
    try:
        return parse_obj_as(timedelta, value)
    except ValidationError:
        return value


def validate_interval_value(
    value: int | timedelta | None,
) -> None:
    """Validate that the given interval value meets required constraints.

    :param value: The interval value to validate.
    :raises ValueError: ValueError: if value is int < 1,
        or timedelta with fractional seconds, or < 1 second.
    """
    if value is None:
        return
    if isinstance(value, int):
        if value < 1:
            raise ValueError(
                f"Interval must be at least 1 second, got {value} seconds",
            )
    else:
        # For timedelta, check that it's at least 1 second
        # and has no fractional seconds
        total_seconds = value.total_seconds()
        if total_seconds != int(total_seconds):
            raise ValueError(
                f"Fractional intervals are not supported, got {total_seconds} seconds",
            )
        if total_seconds < 1:
            raise ValueError(
                f"Interval must be at least 1 second, got {total_seconds} seconds",
            )
