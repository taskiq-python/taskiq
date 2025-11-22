from datetime import timedelta
from typing import Union


def validate_interval_value(
    value: Union[int, timedelta, None],
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
