import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, model_validator
from typing_extensions import Self


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

    @model_validator(mode="after")
    def __check(self) -> Self:
        """
        Validate values.

        This method validates, that either
        `cron`, `interval` or `time` field is present.

        :raises ValueError: if cron, interval and time are none.
        """
        if all(v is None for v in (self.cron, self.interval, self.time)):
            raise ValueError("Either cron, interval or datetime must be present.")
        return self
