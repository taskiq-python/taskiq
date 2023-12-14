import enum


@enum.unique
class WorkerState(int, enum.Enum):
    """Worker state enumeration."""

    READY = enum.auto()
    IDLE = enum.auto()
    BUSY = enum.auto()
    STOPPING = enum.auto()
    STOPPED = enum.auto()
