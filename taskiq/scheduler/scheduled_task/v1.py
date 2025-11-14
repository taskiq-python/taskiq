import uuid
from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field, root_validator


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

    @root_validator(pre=False)  # type: ignore
    @classmethod
    def __check(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        This method validates, that either `cron` or `time` field is present.

        :raises ValueError: if cron and time are none.
        """
        if values.get("cron") is None and values.get("time") is None:
            raise ValueError("Either cron or datetime must be present.")
        return values
