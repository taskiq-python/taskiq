from json import dumps, loads
from typing import Any

from taskiq.abc.serializer import TaskiqSerializer


class JSONSerializer(TaskiqSerializer):
    """Default taskiq serizalizer."""

    def dumpb(self, value: Any) -> bytes:
        """
        Dumps taskiq message to some broker message format.

        :param message: message to send.
        :return: Dumped message.
        """
        return dumps(value).encode()

    def loadb(self, value: bytes) -> Any:
        """
        Parse byte-encoded value received from the wire.

        :param message: value to parse.
        :return: decoded value.
        """
        return loads(value.decode())
