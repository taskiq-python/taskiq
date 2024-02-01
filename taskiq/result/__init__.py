from taskiq.compat import IS_PYDANTIC2

if IS_PYDANTIC2:
    from .v2 import TaskiqResult
else:
    from .v1 import TaskiqResult  # type: ignore


__all__ = [
    "TaskiqResult",
]
