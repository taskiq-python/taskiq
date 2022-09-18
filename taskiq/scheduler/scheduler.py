from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List

from taskiq.abc.broker import AsyncBroker
from taskiq.scheduler.merge_functions import preserve_all

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.schedule_source import ScheduleSource


@dataclass(frozen=True, eq=True)
class ScheduledTask:
    """Abstraction over task schedule."""

    task_name: str
    labels: Dict[str, Any]
    args: List[Any]
    kwargs: Dict[str, Any]
    cron: str


class TaskiqScheduler:
    """Scheduler class."""

    def __init__(
        self,
        broker: AsyncBroker,
        sources: List["ScheduleSource"],
        merge_func: Callable[
            [List["ScheduledTask"], List["ScheduledTask"]],
            List["ScheduledTask"],
        ] = preserve_all,
        refresh_delay: float = 30.0,
    ) -> None:
        self.broker = broker
        self.sources = sources
        self.refresh_delay = refresh_delay
        self.merge_func = merge_func

    async def startup(self) -> None:
        """
        This method is called on startup.

        Here you can do stuff, like creating
        connections or anything you'd like.
        """
        await self.broker.startup()
