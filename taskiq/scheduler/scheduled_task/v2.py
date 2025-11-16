import sys
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, model_validator

from taskiq.scheduler.scheduled_task.validators import validate_interval_value

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class ScheduledTask(BaseModel):
    """Abstraction over task schedule."""

    task_name: str
    labels: Dict[str, Any]
    args: List[Any]
    kwargs: Dict[str, Any]
    task_id: Optional[str] = None
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

        validate_interval_value(self.interval)
        return self
