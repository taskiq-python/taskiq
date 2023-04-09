from abc import ABC, abstractmethod

from taskiq.message import BrokerMessage, TaskiqMessage


class TaskiqFormatter(ABC):
    """Custom formatter for brokers."""

    @abstractmethod
    def dumps(self, message: TaskiqMessage) -> BrokerMessage:
        """
        Dump message to broker message instance.

        :param message: message to send.
        :return: message for brokers.
        """

    @abstractmethod
    def loads(self, message: bytes) -> TaskiqMessage:
        """
        Parses broker message to TaskiqMessage.

        :param message: message to parse.
        :return: parsed taskiq message.
        """
