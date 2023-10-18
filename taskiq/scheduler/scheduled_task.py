import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union


@dataclass
class CronSpec:
    """Cron specification for running tasks."""

    minutes: Optional[Union[str, int]] = "*"
    hours: Optional[Union[str, int]] = "*"
    days: Optional[Union[str, int]] = "*"
    months: Optional[Union[str, int]] = "*"
    weekdays: Optional[Union[str, int]] = "*"

    offset: Optional[Union[str, timedelta]] = None

    def to_cron(self) -> str:
        """Converts cron spec to cron string."""
        return f"{self.minutes} {self.hours} {self.days} {self.months} {self.weekdays}"


@dataclass(frozen=True, eq=True)
class ScheduledTask:
    """Abstraction over task schedule."""

    task_name: str
    labels: Dict[str, Any]
    args: List[Any]
    kwargs: Dict[str, Any]
    schedule_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    cron: Optional[str] = field(default=None)
    cron_offset: Optional[Union[str, timedelta]] = field(default=None)
    time: Optional[datetime] = field(default=None)

    def __post_init__(self) -> None:
        """
        This method validates, that either `cron` or `time` field is present.

        :raises ValueError: if cron and time are none.
        """
        if self.cron is None and self.time is None:
            raise ValueError("Either cron or datetime must be present.")
