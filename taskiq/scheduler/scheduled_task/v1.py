import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, root_validator

from taskiq.scheduler.scheduled_task.validators import validate_interval_value


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

    @root_validator(pre=False)  # type: ignore
    @classmethod
    def __check(cls, values: Dict[str, Any]) -> Dict[str, Any]:
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
