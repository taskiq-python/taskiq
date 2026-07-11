from logging import getLogger
from typing import TYPE_CHECKING, Any

from taskiq.exceptions import ScheduledTaskCancelledError
from taskiq.kicker import AsyncKicker
from taskiq.router import TaskiqRouter
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.utils import maybe_awaitable

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker
    from taskiq.abc.schedule_source import ScheduleSource

logger = getLogger(__name__)


class TaskiqScheduler:
    """Scheduler class."""

    def __init__(
        self,
        broker: "AsyncBroker",
        sources: list["ScheduleSource"],
    ) -> None:  # pragma: no cover
        self.broker = broker
        self.sources = sources

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

        It's triggered on proper time depending on `task.cron`, `task.interval`
        or `task.time` attribute.
        :param source: source that triggered this event.
        :param task: task to send
        """
        try:
            await maybe_awaitable(source.pre_send(task))
        except ScheduledTaskCancelledError:
            logger.info("Scheduled task %s has been cancelled.", task.task_name)
        else:
            kicker: AsyncKicker[Any, Any] = (
                AsyncKicker(task.task_name, self.broker, dict(task.labels))
                .with_labels(schedule_id=task.schedule_id)
                .with_task_id(task_id=task.task_id)
            )
            self._apply_scheduled_route(kicker, task.task_name)
            await kicker.kiq(*task.args, **task.kwargs)
            await maybe_awaitable(source.post_send(task))

    async def shutdown(self) -> None:
        """Shutdown the scheduler process."""
        await self.broker.shutdown()

    def _apply_scheduled_route(
        self,
        kicker: AsyncKicker[Any, Any],
        task_name: str,
    ) -> None:
        """
        Apply scheduler dispatch routing without changing schedule payloads.

        Registered router routes are resolved at send time. If there is no
        router route for the scheduled task, keep the old scheduler behavior and
        send through the scheduler broker instead of the router default broker.
        """
        router = getattr(self.broker, "router", None)
        if isinstance(router, TaskiqRouter) and router.has_route(task_name):
            kicker.with_route(router.resolve_route(task_name))
            return

        kicker.with_broker(self.broker)
