import abc
from typing import Any

from taskiq.result import TaskiqResult


class StreamResultFormatter(abc.ABC):
    @abc.abstractmethod
    def dumps(self, message: TaskiqResult[Any]) -> bytes:
        """
        Dump message to broker message instance.

        :param message: message to send.
        :return: message for brokers.
        """

    @abc.abstractmethod
    def loads(self, message: bytes) -> TaskiqResult[Any]:
        """
        Parses broker message to TaskiqMessage.

        :param message: message to parse.
        :return: parsed taskiq message.
        """
