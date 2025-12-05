from collections.abc import Callable
from json import dumps, loads
from typing import Any

from taskiq.abc.serializer import TaskiqSerializer


class JSONSerializer(TaskiqSerializer):
    """Default taskiq serializer."""

    def __init__(
        self,
        default: Callable[..., None] | None = None,
        ensure_ascii: bool = True,
    ) -> None:
        self.default = default
        self.ensure_ascii = ensure_ascii

    def dumpb(self, value: Any) -> bytes:
        """
        Dumps taskiq message to some broker message format.

        :param value: message to send.
        :return: Dumped message.
        """
        return dumps(
            value,
            default=self.default,
            ensure_ascii=self.ensure_ascii,
        ).encode()

    def loadb(self, value: bytes) -> Any:
        """
        Parse byte-encoded value received from the wire.

        :param value: value to parse.
        :return: decoded value.
        """
        return loads(value.decode())
