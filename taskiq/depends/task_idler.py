from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from taskiq_dependencies import Depends

from taskiq.context import Context


class TaskIdler:
    """Task's dependency to idle task."""

    def __init__(self, context: Context = Depends()) -> None:
        self.context = context

    @asynccontextmanager
    async def __call__(self, timeout: Optional[int] = None) -> AsyncIterator[None]:
        """Idle task."""
        async with self.context.idle(timeout):
            yield
