from pydantic import BaseModel, model_validator

from taskiq.scheduler.scheduled_task.cron import Cron
from taskiq.scheduler.scheduled_task.validators import CronOffset


class CronSpec(BaseModel):
    """Cron specification for running tasks."""

    minutes: str | int | None = "*"
    hours: str | int | None = "*"
    days: str | int | None = "*"
    months: str | int | None = "*"
    weekdays: str | int | None = "*"

    offset: CronOffset | None = None

    @model_validator(mode="after")
    def _validate_cron(self) -> "CronSpec":
        self.to_cron()
        return self

    def to_cron(self) -> Cron:
        """Converts cron spec to a validated cron object."""
        return Cron.parse(
            f"{self.minutes} {self.hours} {self.days} {self.months} {self.weekdays}",
        )
