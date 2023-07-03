# flake8: noqa
from taskiq.compat import pydantic_version

if pydantic_version >= (2, 0):
    from taskiq.result_v2 import TaskiqResult
else:
    from taskiq.result_v1 import TaskiqResult


__all__ = [
    "TaskiqResult",
]
