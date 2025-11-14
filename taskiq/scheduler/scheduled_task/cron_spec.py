from datetime import timedelta

from pydantic import BaseModel


class CronSpec(BaseModel):
    """Cron specification for running tasks."""

    minutes: str | int | None = "*"
    hours: str | int | None = "*"
    days: str | int | None = "*"
    months: str | int | None = "*"
    weekdays: str | int | None = "*"

    offset: str | timedelta | None = None

    def to_cron(self) -> str:  # pragma: no cover
        """Converts cron spec to cron string."""
        return f"{self.minutes} {self.hours} {self.days} {self.months} {self.weekdays}"
