from abc import ABC, abstractmethod
from typing import Any, Dict

from taskiq.message import BrokerMessage, TaskiqMessage


class TaskiqFormatter(ABC):
    """Custom formatter for brokers."""

    @abstractmethod
    def dumps(self, message: TaskiqMessage, labels: Dict[str, Any]) -> BrokerMessage:
        """
        Dump message to broker message instance.

        :param message: message to send.
        :param labels: task's labels.
        :return: message for brokers.
        """

    @abstractmethod
    def loads(self, message: BrokerMessage) -> TaskiqMessage:
        """
        Parses broker message to TaskiqMessage.

        :param message: message to parse.
        :return: parsed taskiq message.
        """
