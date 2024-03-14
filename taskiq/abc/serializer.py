from abc import ABC, abstractmethod
from typing import Any


class TaskiqSerializer(ABC):
    """Custom serializer for brokers."""

    @abstractmethod
    def dumpb(self, value: Any) -> bytes:
        """
        Dump value to bytes for sending through the wire.

        :param value: value to encode.
        :return: encoded value.
        """

    @abstractmethod
    def loadb(self, value: bytes) -> Any:
        """
        Parse byte-encoded value received from the wire.

        :param value: value to decode.
        :return: decoded value.
        """
