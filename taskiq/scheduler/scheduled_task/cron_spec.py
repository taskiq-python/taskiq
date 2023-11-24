from datetime import timedelta
from typing import Optional, Union

from pydantic import BaseModel


class CronSpec(BaseModel):
    """Cron specification for running tasks."""

    minutes: Optional[Union[str, int]] = "*"
    hours: Optional[Union[str, int]] = "*"
    days: Optional[Union[str, int]] = "*"
    months: Optional[Union[str, int]] = "*"
    weekdays: Optional[Union[str, int]] = "*"

    offset: Optional[Union[str, timedelta]] = None

    def to_cron(self) -> str:  # pragma: no cover
        """Converts cron spec to cron string."""
        return f"{self.minutes} {self.hours} {self.days} {self.months} {self.weekdays}"
