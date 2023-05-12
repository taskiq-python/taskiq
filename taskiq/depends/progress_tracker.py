import enum
from typing import Generic, Union, overload

from pydantic.generics import GenericModel
from taskiq_dependencies import Depends
from typing_extensions import TypeVar

from taskiq.context import Context

# TODO: PEP 696
# _ProgressType = TypeVar("_ProgressType", default=Any) #  noqa: E800
_ProgressType = TypeVar("_ProgressType")


class Undefined(enum.Enum):
    """For undefined value."""

    undefined = ...


undefined = Undefined.undefined


class TaskState(str, enum.Enum):  # noqa: WPS600
    """State of task execution."""

    PENDING = "PENDING"
    RECEIVED = "RECEIVED"
    STARTED = "STARTED"
    FAILURE = "FAILURE"
    SUCCESS = "SUCCESS"
    RETRY = "RETRY"


class TaskProgress(GenericModel, Generic[_ProgressType]):
    """Progress of task execution."""

    state: Union[TaskState, str]
    meta: _ProgressType


class ProgressTracker(Generic[_ProgressType]):
    """Task's dependency to set progress."""

    def __init__(
        self,
        context: Context = Depends(),  # noqa: WPS404
    ):
        self.context = context

    @overload
    async def set_progress(
        self,
        state: TaskProgress[_ProgressType],
    ) -> None:  # pragma: no cover
        """
        Set progress of task execution.

        :param state: TaskProgress value.
        """
        ...

    @overload
    async def set_progress(
        self,
        state: Union[TaskState, str],
        meta: _ProgressType,
    ) -> None:  # pragma: no cover
        """
        Set progress of task execution.

        :param state: TaskState value.
        :param meta: progress of task.
        """
        ...

    async def set_progress(
        self,
        state: Union[TaskProgress[_ProgressType], TaskState, str],
        meta: Union[_ProgressType, Undefined] = undefined,
    ) -> None:
        """Set progress.

        :param state: TaskState or str
        :param meta: progress data
        :raises ValueError: if meta is bad
        """
        if isinstance(state, (TaskState, str)):
            if meta is undefined:
                raise ValueError("meta is undefined")

            progress = TaskProgress(state=state, meta=meta)
        else:
            progress = state

        await self.context.broker.result_backend.set_progress(
            self.context.message.task_id,
            progress,
        )
