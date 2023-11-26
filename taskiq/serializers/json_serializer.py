from json import dumps, loads
from typing import Any, Callable, Optional

from taskiq.abc.serializer import TaskiqSerializer


class JSONSerializer(TaskiqSerializer):
    """Default taskiq serizalizer."""

    def __init__(self, default: Optional[Callable[..., None]] = None) -> None:
        self.default = default

    def dumpb(self, value: Any) -> bytes:
        """
        Dumps taskiq message to some broker message format.

        :param message: message to send.
        :return: Dumped message.
        """
        return dumps(
            value,
            default=self.default,
        ).encode()

    def loadb(self, value: bytes) -> Any:
        """
        Parse byte-encoded value received from the wire.

        :param message: value to parse.
        :return: decoded value.
        """
        return loads(value.decode())
