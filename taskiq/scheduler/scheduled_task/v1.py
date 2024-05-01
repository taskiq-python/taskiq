import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, root_validator

from taskiq.utils import get_present_object_fields


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
    period: Optional[float | int] = None

    @root_validator(pre=False)  # type: ignore
    @classmethod
    def __check(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        This method validates, that either `cron` or `time` field is present.

        :raises ValueError: if cron and time are none.
        """
        required_fields = ("cron", "time", "period")
        present_fields = get_present_object_fields(values, required_fields)
        if not present_fields:
            message = f"At least one of {required_fields} must be set."
            raise ValueError(message)
        return values
