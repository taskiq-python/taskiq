from collections.abc import Coroutine
from types import CoroutineType
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.schedule_source import ScheduleSource
    from taskiq.scheduler.scheduled_task import ScheduledTask
    from taskiq.scheduler.scheduler import TaskiqScheduler


class SchedulerPlugin:  # pragma: no cover
    """
    Base class for scheduler plugins.

    Scheduler plugins observe the scheduler's lifecycle.
    They receive notifications when schedules are refreshed
    from a source and when a scheduled task is sent.

    Plugins are observers: unlike schedule sources they cannot
    cancel or modify tasks. Exceptions raised from event hooks
    (`on_schedules_updated`, `pre_send`, `post_send`) are logged
    and suppressed, so a failing plugin never breaks scheduling.
    Exceptions from `startup` are propagated to fail fast on boot.

    All hooks can be either sync or async.
    """

    def __init__(self) -> None:
        self.scheduler: TaskiqScheduler = None  # type: ignore

    def set_scheduler(self, scheduler: "TaskiqScheduler") -> None:
        """
        Sets scheduler to plugin.

        This gives the plugin access to the broker
        and all schedule sources of the scheduler.

        :param scheduler: scheduler to set.
        """
        self.scheduler = scheduler

    def startup(
        self,
    ) -> Union[None, Coroutine[Any, Any, None], "CoroutineType[Any, Any, None]"]:
        """
        Startup method to perform various action during startup.

        This function can be either sync or async.
        Executed during scheduler's startup, after the broker's startup.

        :returns nothing.
        """

    def shutdown(
        self,
    ) -> Union[None, Coroutine[Any, Any, None], "CoroutineType[Any, Any, None]"]:
        """
        Shutdown method to perform various action during shutdown.

        This function can be either sync or async.
        Executed during scheduler's shutdown, before the broker's shutdown.

        :returns nothing.
        """

    def on_schedules_updated(
        self,
        source: "ScheduleSource",
        schedules: "list[ScheduledTask]",
    ) -> Union[None, Coroutine[Any, Any, None], "CoroutineType[Any, Any, None]"]:
        """
        This hook is called when schedules are refreshed from a source.

        The scheduler refreshes all its sources on startup and
        then periodically, based on its update interval. This hook
        receives the full list of schedules a source returned.

        :param source: source the schedules were fetched from.
        :param schedules: all schedules the source returned.
        """

    def pre_send(
        self,
        source: "ScheduleSource",
        task: "ScheduledTask",
    ) -> Union[None, Coroutine[Any, Any, None], "CoroutineType[Any, Any, None]"]:
        """
        This hook is called right before a scheduled task is sent.

        It is not called if the source's own `pre_send`
        cancelled the task.

        :param source: source that triggered this task.
        :param task: task that is about to be sent.
        """

    def post_send(
        self,
        source: "ScheduleSource",
        task: "ScheduledTask",
    ) -> Union[None, Coroutine[Any, Any, None], "CoroutineType[Any, Any, None]"]:
        """
        This hook is called right after a scheduled task is sent.

        :param source: source that triggered this task.
        :param task: task that has been sent.
        """
