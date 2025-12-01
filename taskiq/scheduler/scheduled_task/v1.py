import uuid
from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field, root_validator

from taskiq.scheduler.scheduled_task.validators import validate_interval_value


class ScheduledTask(BaseModel):
    """Abstraction over task schedule."""

    task_name: str
    labels: dict[str, Any]
    args: list[Any]
    kwargs: dict[str, Any]
    task_id: str | None = None
    schedule_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    cron: str | None = None
    cron_offset: str | timedelta | None = None
    time: datetime | None = None
    interval: int | timedelta | None = None

    @root_validator(pre=False)  # type: ignore
    @classmethod
    def __check(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate.

        This method validates,
        that either `cron`, `interval` or `time` field is present.
        For interval tasks, validates that interval is at least 1 second
        and has no fractional seconds.

        :raises ValueError: if cron, interval and time are none, or interval is invalid.
        """
        if all(values.get(key) is None for key in ("cron", "interval", "time")):
            raise ValueError("Either cron, interval, or datetime must be present.")

        validate_interval_value(values.get("interval"))
        return values
