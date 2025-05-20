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
        This method validates, that either `cron`, `time` or `interval` field is present.

        :raises ValueError: if cron, time and interval are none.
        """
        if (
            values.get("cron") is None
            and values.get("time") is None
            and values.get("interval") is None
        ):
            raise ValueError("Either cron, datetime or interval must be present.")
        return values
