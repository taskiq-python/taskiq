import enum


class LogLevel(str, enum.Enum):
    """Different log levels."""

    INFO = "INFO"
    WARNING = "WARNING"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    FATAL = "FATAL"
