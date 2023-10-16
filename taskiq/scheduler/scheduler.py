from typing import TYPE_CHECKING, Callable, List

from taskiq.kicker import AsyncKicker
from taskiq.scheduler.merge_functions import only_new
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.utils import maybe_awaitable

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker
    from taskiq.abc.schedule_source import ScheduleSource


class TaskiqScheduler:
    """Scheduler class."""

    def __init__(
        self,
        broker: "AsyncBroker",
        sources: List["ScheduleSource"],
        merge_func: Callable[
            [List["ScheduledTask"], List["ScheduledTask"]],
            List["ScheduledTask"],
        ] = only_new,
        refresh_delay: float = 30.0,
    ) -> None:  # pragma: no cover
        self.broker = broker
        self.sources = sources
        self.refresh_delay = refresh_delay
        self.merge_func = merge_func

    async def startup(self) -> None:  # pragma: no cover
        """
        This method is called on startup.

        Here you can do stuff, like creating
        connections or anything you'd like.
        """
        await self.broker.startup()

    async def on_ready(self, source: "ScheduleSource", task: ScheduledTask) -> None:
        """
        This method is called when task is ready to be enqueued.

        It's triggered on proper time depending on `task.cron` or `task.time` attribute.
        :param task: task to send
        """
        await maybe_awaitable(source.pre_send(task))
        await AsyncKicker(task.task_name, self.broker, task.labels).kiq(
            *task.args,
            **task.kwargs,
        )
        await maybe_awaitable(source.post_send(task))

    async def shutdown(self) -> None:
        """Shutdown the scheduler process."""
        await self.broker.shutdown()
