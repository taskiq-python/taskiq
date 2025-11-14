import sys
import uuid
from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field, model_validator

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


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

    @model_validator(mode="after")
    def __check(self) -> Self:
        """
        This method validates, that either `cron` or `time` field is present.

        :raises ValueError: if cron and time are none.
        """
        if self.cron is None and self.time is None:
            raise ValueError("Either cron or datetime must be present.")
        return self
