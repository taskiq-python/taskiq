from typing import Any, Dict, List

from pydantic import BaseModel


class TaskiqMessage(BaseModel):
    """
    Message abstractions.

    This an internal class used
    by brokers. Every remote call
    receive such messages.
    """

    task_id: str
    task_name: str
    labels: Dict[str, Any]
    args: List[Any]
    kwargs: Dict[str, Any]


class BrokerMessage(BaseModel):
    """Format of messages for brokers."""

    task_id: str
    task_name: str
    message: bytes
    labels: Dict[str, Any]
