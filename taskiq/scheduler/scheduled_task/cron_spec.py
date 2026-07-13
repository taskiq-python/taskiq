from collections.abc import Callable
from datetime import timedelta
from typing import Any

from pydantic import BaseModel

from taskiq.compat import IS_PYDANTIC2
from taskiq.scheduler.scheduled_task.validators import parse_cron_offset

_offset_before_validator: Callable[..., Any]
if IS_PYDANTIC2:
    from pydantic import field_validator

    _offset_before_validator = field_validator("offset", mode="before")
else:
    from pydantic import validator

    _offset_before_validator = validator("offset", pre=True)


class CronSpec(BaseModel):
    """Cron specification for running tasks."""

    minutes: str | int | None = "*"
    hours: str | int | None = "*"
    days: str | int | None = "*"
    months: str | int | None = "*"
    weekdays: str | int | None = "*"

    offset: str | timedelta | None = None

    @_offset_before_validator
    @classmethod
    def _parse_offset(cls, value: Any) -> Any:
        return parse_cron_offset(value)

    def to_cron(self) -> str:  # pragma: no cover
        """Converts cron spec to cron string."""
        return f"{self.minutes} {self.hours} {self.days} {self.months} {self.weekdays}"
