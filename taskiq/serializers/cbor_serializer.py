import datetime
from collections.abc import Callable
from typing import Any

from taskiq.abc.serializer import TaskiqSerializer

try:
    import cbor2
except ImportError:
    cbor2 = None  # type: ignore


class CBORSerializer(TaskiqSerializer):
    """
    Taskiq serializer using cbor2 library.

    See https://cbor2.readthedocs.io/en/stable/ for more information.
    """

    def __init__(
        self,
        datetime_as_timestamp: bool = True,
        timezone: datetime.tzinfo | None = None,
        value_sharing: bool = False,
        default: Callable[[Any, Any], Any] | None = None,
        canonical: bool = False,
        date_as_datetime: bool = True,
        string_referencing: bool = True,
        # Decoder options
        tag_hook: Callable[["cbor2.CBORDecoder", Any], Any] | None = None,
        object_hook: Callable[["cbor2.CBORDecoder", dict[Any, Any]], Any] | None = None,
    ) -> None:
        if cbor2 is None:
            raise ImportError("cbor2 is not installed")
        self.datetime_as_timestamp = datetime_as_timestamp
        self.timezone = timezone
        self.value_sharing = value_sharing
        self.default = default
        self.canonical = canonical
        self.date_as_datetime = date_as_datetime
        self.string_referencing = string_referencing
        self.tag_hook = tag_hook
        self.object_hook = object_hook

    def dumpb(self, value: Any) -> bytes:
        """Dump value to bytes."""
        return cbor2.dumps(  # type: ignore
            value,
            datetime_as_timestamp=self.datetime_as_timestamp,
            timezone=self.timezone,
            value_sharing=self.value_sharing,
            default=self.default,
            canonical=self.canonical,
            date_as_datetime=self.date_as_datetime,
            string_referencing=self.string_referencing,
        )

    def loadb(self, value: bytes) -> Any:
        """Load value from bytes."""
        return cbor2.loads(  # type: ignore
            value,
            tag_hook=self.tag_hook,
            object_hook=self.object_hook,
        )
