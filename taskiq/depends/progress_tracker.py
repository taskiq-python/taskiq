import enum
from typing import Generic, Optional, TypeVar, Union

from taskiq_dependencies import Depends

from taskiq.compat import IS_PYDANTIC2
from taskiq.context import Context

_ProgressType = TypeVar("_ProgressType")


class TaskState(str, enum.Enum):
    """State of task execution."""

    STARTED = "STARTED"
    FAILURE = "FAILURE"
    SUCCESS = "SUCCESS"
    RETRY = "RETRY"


if IS_PYDANTIC2:
    from pydantic import BaseModel, ConfigDict

    class _TaskProgressConfig(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)

else:
    from pydantic.generics import GenericModel

    class _TaskProgressConfig(GenericModel):  # type: ignore[no-redef]
        class Config:
            arbitrary_types_allowed = True


class TaskProgress(_TaskProgressConfig, Generic[_ProgressType]):
    """Progress of task execution."""

    state: Union[TaskState, str]
    meta: Optional[_ProgressType]


class ProgressTracker(Generic[_ProgressType]):
    """Task's dependency to set progress."""

    def __init__(
        self,
        context: Context = Depends(),
    ) -> None:
        self.context = context

    async def set_progress(
        self,
        state: Union[TaskState, str],
        meta: Optional[_ProgressType] = None,
    ) -> None:
        """Set progress.

        :param state: TaskState or str
        :param meta: progress data
        """
        if meta is None:
            progress = await self.get_progress()
            meta = progress.meta if progress else None

        progress = TaskProgress(
            state=state,
            meta=meta,
        )

        await self.context.broker.result_backend.set_progress(
            self.context.message.task_id,
            progress,
        )

    async def get_progress(self) -> Optional[TaskProgress[_ProgressType]]:
        """Get progress."""
        return await self.context.broker.result_backend.get_progress(
            self.context.message.task_id,
        )
