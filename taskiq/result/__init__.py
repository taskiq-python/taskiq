# flake8: noqa
from packaging.version import Version

from taskiq.compat import PYDANTIC_VER

if PYDANTIC_VER >= Version("2.0"):
    from .v2 import TaskiqResult
else:
    from .v1 import TaskiqResult


__all__ = [
    "TaskiqResult",
]
