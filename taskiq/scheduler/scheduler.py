from __future__ import annotations

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
        broker: AsyncBroker,
        sources: list[ScheduleSource],
    ) -> None:  # pragma: no cover
        self.broker = broker
        self.sources = sources
        self._started_brokers: list[AsyncBroker] = []

    async def startup(self) -> None:
        """
        Start every producer broker the scheduler may route through.

        A scheduler with a standalone broker preserves the legacy one-broker
        lifecycle. When brokers share a router, the scheduler owns all of their
        producer-side resources because late route resolution may select any of
        them.
        """
        if self._started_brokers:
            raise RuntimeError("TaskiqScheduler is already started.")

        try:
            for broker in self._managed_brokers():
                broker.is_scheduler_process = True
                self._started_brokers.append(broker)
                await broker.startup()
        except BaseException:
            cleanup_error = await self._shutdown_brokers(
                tuple(self._started_brokers),
            )
            self._started_brokers.clear()
            if cleanup_error is not None:
                logger.error(
                    "Error while cleaning up partially started scheduler brokers.",
                    exc_info=(
                        type(cleanup_error),
                        cleanup_error,
                        cleanup_error.__traceback__,
                    ),
                )
            raise

    async def on_ready(self, source: ScheduleSource, task: ScheduledTask) -> None:
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
        """Shut down scheduler brokers in reverse startup order."""
        brokers = tuple(self._started_brokers)
        if not brokers:
            brokers = (self.broker,)
        self._started_brokers.clear()
        shutdown_error = await self._shutdown_brokers(brokers)
        if shutdown_error is not None:
            raise shutdown_error

    def _managed_brokers(self) -> tuple[AsyncBroker, ...]:
        """Return scheduler-owned producer brokers in startup order."""
        router = getattr(self.broker, "router", None)
        if isinstance(router, TaskiqRouter):
            brokers = tuple(router.brokers.values())
            if brokers:
                return brokers
        return (self.broker,)

    @staticmethod
    async def _shutdown_brokers(
        brokers: tuple[AsyncBroker, ...],
    ) -> BaseException | None:
        """Close all brokers while preserving the first shutdown failure."""
        first_error: BaseException | None = None
        for broker in reversed(brokers):
            try:
                await broker.shutdown()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
                else:
                    logger.error(
                        "Additional error while shutting down scheduler brokers.",
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )
        return first_error

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
