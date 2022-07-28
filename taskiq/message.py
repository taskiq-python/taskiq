from typing import Any, Dict, List

from pydantic import BaseModel


class TaskiqMessage(BaseModel):
    """
    Message abstractions.

    This an internal class used
    by brokers. Every remote call
    recieve such messages.
    """

    task_id: str
    task_name: str
    meta: Dict[str, str]
    args: List[Any]
    kwargs: Dict[str, Any]
