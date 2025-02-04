import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, root_validator


class ScheduledTask(BaseModel):
    """Abstraction over task schedule."""

    task_name: str
    labels: Dict[str, Any]
    args: List[Any]
    kwargs: Dict[str, Any]
    schedule_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    cron: Optional[str] = None
    cron_offset: Optional[Union[str, timedelta]] = None
    time: Optional[datetime] = None
    interval: Optional[int] = None

    @root_validator(pre=False)  # type: ignore
    @classmethod
    def __check(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate values.

        This method validates, that either
        `cron`, `interval` or `time` field is present.

        :raises ValueError: if cron, interval and time are none.
        """
        if all(values.get(v) is None for v in ("cron", "interval", "time")):
            raise ValueError("Either cron, interval or datetime must be present.")
        return values
