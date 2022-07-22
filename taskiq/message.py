from dataclasses import dataclass
from typing import Any, Dict, Tuple


@dataclass
class TaskiqMessage:
    """
    Message abstractions.

    This an internal class used
    by brokers. Every remote call
    recieve such messages.
    """

    task_name: str
    headers: Dict[str, str]
    meta: Dict[str, str]
    args: Tuple[Any]
    kwargs: Dict[str, Any]
