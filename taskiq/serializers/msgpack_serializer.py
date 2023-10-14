from typing import Any, Callable, Optional

from taskiq.abc.serializer import TaskiqSerializer

try:
    import msgpack
except ImportError:
    msgpack = None  # type: ignore


class MSGPackSerializer(TaskiqSerializer):
    """Taskiq serializer using msgpack library."""

    def __init__(
        self,
        default: Optional[Callable[[Any], Any]] = None,
        use_single_float: bool = False,
        use_bin_type: bool = True,
        datetime: bool = True,
    ) -> None:
        if msgpack is None:
            raise ImportError("msgpack is not installed")
        self.default = default
        self.use_single_float = use_single_float
        self.use_bin_type = use_bin_type
        self.datetime = datetime

    def dumpb(self, value: Any) -> bytes:
        """Dump value to bytes."""
        return msgpack.packb(
            value,
            default=self.default,
            use_single_float=self.use_single_float,
            use_bin_type=self.use_bin_type,
            datetime=self.datetime,
        )

    def loadb(self, value: bytes) -> Any:
        """Load value from bytes."""
        return msgpack.unpackb(value, timestamp=3)
