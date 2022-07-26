from typing import Any, Dict, Tuple

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
    args: Tuple[Any]
    kwargs: Dict[str, Any]
