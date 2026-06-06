from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.decor import AsyncTaskiqDecoratedTask

__all__ = ("resolve_task_name",)


def resolve_task_name(task: str | AsyncTaskiqDecoratedTask[Any, Any]) -> str:
    """Resolve a task reference to a task name."""
    if isinstance(task, str):
        return task
    task_name = getattr(task, "task_name", None)
    if isinstance(task_name, str):
        return task_name
    raise TypeError("Route task must be a task name or decorated task.")
