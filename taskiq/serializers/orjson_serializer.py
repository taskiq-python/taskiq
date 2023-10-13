from typing import Any, Callable, Optional

from taskiq.abc.serializer import TaskiqSerializer

try:
    import orjson
except ImportError:
    orjson = None  # type: ignore


class ORJSONSerializer(TaskiqSerializer):
    """Taskiq serializer using orjson library."""

    def __init__(
        self,
        default: Optional[Callable[[Any], Any]] = None,
        option: Optional[int] = None,
    ) -> None:
        if orjson is None:
            raise ImportError("orjson is not installed")
        self.default = default
        self.option = option

    def dumpb(self, value: Any) -> bytes:
        """Dump value to bytes."""
        return orjson.dumps(
            value,
            default=self.default,
            option=self.option,
        )

    def loadb(self, value: bytes) -> Any:
        """Load value from bytes."""
        return orjson.loads(value)
