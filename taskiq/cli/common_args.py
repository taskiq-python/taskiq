import enum


class LogLevel(str, enum.Enum):  # noqa: WPS600
    """Different log levels."""

    INFO = "INFO"
    WARNING = "WARNING"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    FATAL = "FATAL"
