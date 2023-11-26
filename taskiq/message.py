from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from taskiq.labels import parse_label


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
    labels_types: Optional[Dict[str, int]] = None
    args: List[Any]
    kwargs: Dict[str, Any]

    def parse_labels(self) -> None:
        """
        Parse labels.

        :return: None
        """
        if self.labels_types is None:
            return

        for label, label_type in self.labels_types.items():
            if label in self.labels:
                self.labels[label] = parse_label(self.labels[label], label_type)


class BrokerMessage(BaseModel):
    """Format of messages for brokers."""

    task_id: str
    task_name: str
    message: bytes
    labels: Dict[str, Any]
