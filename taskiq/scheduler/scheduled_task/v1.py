import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, root_validator


class ScheduledTask(BaseModel):
    """Abstraction over task schedule."""

    task_name: str
    labels: Dict[str, Any]
    args: List[Any]
    kwargs: Dict[str, Any]
    schedule_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    cron: Optional[str] = None
    cron_offset: Optional[Union[str, timedelta]] = None
    time: Optional[datetime] = None
    interval: Union[int, timedelta, None] = None

    @root_validator(pre=False)  # type: ignore
    @classmethod
    def __check(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate.

        This method validates,
        that either `cron`, `interval` or `time` field is present.
        For interval tasks, validates that interval is at least 1 second
        and has no fractional seconds.

        :raises ValueError: if cron, interval and time are none, or interval is invalid.
        """
        if not {"cron", "interval", "time"} & values.keys():
            raise ValueError("Either cron, interval, or datetime must be present.")

        # Validate interval constraints
        if "interval" in values and values["interval"] is not None:
            interval = values["interval"]
            if isinstance(interval, int):
                if interval < 1:
                    raise ValueError(
                        f"Interval must be at least 1 second, got {interval} seconds",
                    )
            else:
                # For timedelta, check that it's at least 1 second
                # and has no fractional seconds
                total_seconds = interval.total_seconds()
                if total_seconds != int(total_seconds):
                    raise ValueError(
                        f"Fractional intervals are not supported, "
                        f"got {total_seconds} seconds",
                    )
                if total_seconds < 1:
                    raise ValueError(
                        f"Interval must be at least 1 second, "
                        f"got {total_seconds} seconds",
                    )

        return values
