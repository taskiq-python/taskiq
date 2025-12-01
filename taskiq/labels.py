import base64
import enum
from collections.abc import Callable
from typing import Any


class LabelType(enum.IntEnum):
    """Possible label types."""

    ANY = enum.auto()
    INT = enum.auto()
    STR = enum.auto()
    FLOAT = enum.auto()
    BOOL = enum.auto()
    BYTES = enum.auto()


_LABEL_PARSERS: dict[LabelType, Callable[[str], Any]] = {
    LabelType.INT: int,
    LabelType.STR: str,
    LabelType.FLOAT: float,
    LabelType.BOOL: lambda x: str(x).lower() == "true",
    LabelType.BYTES: base64.b64decode,
    LabelType.ANY: lambda x: x,
}


def prepare_label(label_value: Any) -> tuple[str, int]:
    """
    Prepare label value for serialization.

    :param label_value: label value to prepare.
    :return: tuple of prepared label value and its type.
    """
    var_type = type(label_value)
    if var_type in (int, str, float, bool):
        return str(label_value), LabelType[var_type.__name__.upper()].value
    if var_type is bytes:
        return base64.b64encode(label_value).decode(), LabelType.BYTES.value
    return str(label_value), LabelType.ANY.value


def parse_label(label_value: Any, label_type: int | None = None) -> Any:
    """
    Parse label value from serialized format.

    :param label_value: label value to parse.
    :param label_type: label type.
    :return: parsed label value.
    """
    if label_type is None:
        return label_value
    label_type = LabelType(label_type)
    if label_type in _LABEL_PARSERS:
        return _LABEL_PARSERS[label_type](label_value)
    raise ValueError(f"Unsupported label type: {label_type}")
