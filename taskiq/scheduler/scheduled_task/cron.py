from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Annotated, Any

from pydantic import PlainSerializer, PlainValidator, WithJsonSchema


class CronError(ValueError):
    """Raised on invalid cron expression."""


class WeekDays(str, Enum):
    """Weekday names."""

    SUN = "SUN"
    MON = "MON"
    TUE = "TUE"
    WED = "WED"
    THU = "THU"
    FRI = "FRI"
    SAT = "SAT"


class Months(str, Enum):
    """Month names."""

    JAN = "JAN"
    FEB = "FEB"
    MAR = "MAR"
    APR = "APR"
    MAY = "MAY"
    JUN = "JUN"
    JUL = "JUL"
    AUG = "AUG"
    SEP = "SEP"
    OCT = "OCT"
    NOV = "NOV"
    DEC = "DEC"


@dataclass(frozen=True)
class FieldSpec:
    """Allowed values of a cron field."""

    name: str
    low: int
    high: int
    aliases: Mapping[str, int] = field(default_factory=dict)


MONTH_NAMES: dict[str, int] = {n.value: i for i, n in enumerate(Months, start=1)}
WEEKDAY_NAMES: dict[str, int] = {n.value: i for i, n in enumerate(WeekDays)}

MINUTE = FieldSpec("minute", 0, 59)
HOUR = FieldSpec("hour", 0, 23)
DAY = FieldSpec("day", 1, 31)
MONTH = FieldSpec("month", 1, 12, MONTH_NAMES)
WEEKDAY = FieldSpec("weekday", 0, 7, WEEKDAY_NAMES)  # 0 and 7 = SUN


def _is_uint(s: str) -> bool:
    return s.isascii() and s.isdigit()


@dataclass(frozen=True)
class CronField:
    """
    Single cron field.

    Accepted values:
    *: any value
    N: one value
    A-B: a range, A <= B
    */S: a step within the whole range
    A-B/S: a step inside the range
    Comma separated lists of the above.
    """

    value: str
    spec: FieldSpec

    def __post_init__(self) -> None:
        for part in self.value.split(","):
            self._check_part(part)

    def _check_part(self, part: str) -> None:
        name = self.spec.name

        base, has_step, step = part.partition("/")
        if has_step:
            if not _is_uint(step) or int(step) == 0:
                raise CronError(f"{name}: the step should be > 0, got {step!r}")
            if base != "*" and "-" not in base:
                raise CronError(
                    f"{name}: the step is allowed only after '*' or a range, "
                    f"got {part!r}",
                )

        if base == "*":
            return

        start, has_range, end = base.partition("-")
        lo = self._to_number(start)
        if has_range:
            hi = self._to_number(end)
            if lo > hi:
                raise CronError(f"{name}: start must be lower than the end {part!r}")

    def _to_number(self, token: str) -> int:
        spec = self.spec
        alias = token.upper()
        if alias in spec.aliases:
            return spec.aliases[alias]
        if not _is_uint(token):
            raise CronError(f"{spec.name}: a number expected, got {token!r}")
        n = int(token)
        if not spec.low <= n <= spec.high:
            raise CronError(f"{spec.name}: {n} is out of range {spec.low}-{spec.high}")
        return n

    def to_numeric(self) -> str:
        """Convert the field to numbers only, in the form pycron understands."""
        is_weekday = self.spec is WEEKDAY
        parts: list[str] = []
        for part in self.value.split(","):
            base, _, step = part.partition("/")
            if base == "*":
                parts.append(part)
                continue
            start, has_range, end = base.partition("-")
            lo = self._to_number(start)
            if not has_range:
                parts.append(str(lo % 7 if is_weekday else lo))
            elif is_weekday:
                values = range(lo, self._to_number(end) + 1, int(step or 1))
                parts.extend(str(v % 7) for v in values)
            else:
                suffix = f"/{step}" if step else ""
                parts.append(f"{lo}-{self._to_number(end)}{suffix}")
        return ",".join(parts)


@dataclass
class Cron:
    """Validated 5-field cron expression."""

    minute: CronField
    hour: CronField
    day: CronField
    month: CronField
    weekday: CronField

    def __post_init__(self) -> None:
        expected = (MINUTE, HOUR, DAY, MONTH, WEEKDAY)
        actual = (self.minute, self.hour, self.day, self.month, self.weekday)
        for f, spec in zip(actual, expected, strict=True):
            if f.spec is not spec:
                raise CronError(
                    f"field {spec.name} is created with wrong spec: {f.spec.name}",
                )

    @classmethod
    def parse(cls, expr: str) -> "Cron":
        """
        Parse cron string.

        :param expr: cron expression with 5 fields.
        :raises CronError: if the expression is invalid.
        :return: cron object.
        """
        parts = expr.split()
        if len(parts) != 5:
            raise CronError(f"Need 5 fields, got: {len(parts)}: {expr!r}")

        specs = (MINUTE, HOUR, DAY, MONTH, WEEKDAY)
        return cls(*(CronField(v, s) for v, s in zip(parts, specs, strict=True)))

    def to_pycron(self) -> str:
        """Cron string with numeric values only, suitable for pycron."""
        fields = (self.minute, self.hour, self.day, self.month, self.weekday)
        return " ".join(f.to_numeric() for f in fields)

    def __str__(self) -> str:
        return (
            f"{self.minute.value} {self.hour.value} {self.day.value} "
            f"{self.month.value} {self.weekday.value}"
        )


def _validate_cron(value: Any) -> Cron:
    if isinstance(value, Cron):
        return value
    if isinstance(value, str):
        return Cron.parse(value)
    raise CronError(f"cron must be a string or Cron, got {type(value).__name__}")


CronExpression = Annotated[
    Cron | str,
    PlainValidator(_validate_cron),
    PlainSerializer(str, return_type=str),
    WithJsonSchema({"type": "string"}),
]
