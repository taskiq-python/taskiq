import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, model_validator
from typing_extensions import Self


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

    @model_validator(mode="after")
    def __check(self) -> Self:
        """Validate.

        This method validates,
        that either `cron`, `interval` or `time` field is present.
        For interval tasks, validates that interval is at least 1 second
        and has no fractional seconds.

        :raises ValueError: if cron, interval and time are none, or interval is invalid.
        """
        if self.cron is None and self.time is None and self.interval is None:
            raise ValueError("Either cron, interval, or datetime must be present.")

        # Validate interval constraints
        if self.interval is not None:
            if isinstance(self.interval, int):
                if self.interval < 1:
                    raise ValueError(
                        f"Interval must be at least 1 second, "
                        f"got {self.interval} seconds",
                    )
            else:
                # For timedelta, check that it's at least 1 second
                # and has no fractional seconds
                total_seconds = self.interval.total_seconds()
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

        return self
