from logging import getLogger
from typing import TYPE_CHECKING, Any

from taskiq.exceptions import ScheduledTaskCancelledError
from taskiq.kicker import AsyncKicker
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.utils import maybe_awaitable

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker
    from taskiq.abc.schedule_source import ScheduleSource
    from taskiq.abc.scheduler_plugin import SchedulerPlugin

logger = getLogger(__name__)


class TaskiqScheduler:
    """Scheduler class."""

    def __init__(
        self,
        broker: "AsyncBroker",
        sources: list["ScheduleSource"],
        plugins: "list[SchedulerPlugin] | None" = None,
    ) -> None:  # pragma: no cover
        self.broker = broker
        self.sources = sources
        self.plugins = plugins or []
        for plugin in self.plugins:
            plugin.set_scheduler(self)

    async def startup(self) -> None:  # pragma: no cover
        """
        This method is called on startup.

        Here you can do stuff, like creating
        connections or anything you'd like.
        """
        await self.broker.startup()
        for plugin in self.plugins:
            await maybe_awaitable(plugin.startup())

    async def _emit(self, hook: str, *args: Any) -> None:
        """
        Call a hook on every plugin, suppressing errors.

        Plugins are observers, so a failing plugin
        must never break scheduling. Exceptions are
        logged and suppressed.

        :param hook: name of the hook to call.
        :param args: arguments to pass to the hook.
        """
        for plugin in self.plugins:
            try:
                await maybe_awaitable(getattr(plugin, hook)(*args))
            except Exception:
                logger.exception(
                    "Scheduler plugin %s failed in %s hook.",
                    type(plugin).__name__,
                    hook,
                )

    async def on_schedules_updated(
        self,
        source: "ScheduleSource",
        schedules: list[ScheduledTask],
    ) -> None:
        """
        This method is called when schedules are refreshed from a source.

        It notifies all plugins with the full list of
        schedules the source returned.

        :param source: source the schedules were fetched from.
        :param schedules: all schedules the source returned.
        """
        await self._emit("on_schedules_updated", source, schedules)

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
            await self._emit("pre_send", source, task)
            await (
                AsyncKicker(task.task_name, self.broker, task.labels)
                .with_labels(
                    schedule_id=task.schedule_id,
                )
                .with_task_id(task_id=task.task_id)
                .kiq(
                    *task.args,
                    **task.kwargs,
                )
            )
            await maybe_awaitable(source.post_send(task))
            await self._emit("post_send", source, task)

    async def shutdown(self) -> None:
        """Shutdown the scheduler process."""
        for plugin in self.plugins:
            try:
                await maybe_awaitable(plugin.shutdown())
            except Exception:
                logger.exception(
                    "Scheduler plugin %s failed to shut down.",
                    type(plugin).__name__,
                )
        await self.broker.shutdown()
