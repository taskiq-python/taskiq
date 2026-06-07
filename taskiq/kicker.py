from __future__ import annotations

from collections.abc import Coroutine
from dataclasses import replace
from datetime import datetime, timedelta
from logging import getLogger
from types import CoroutineType
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    ParamSpec,
    TypeVar,
    cast,
    overload,
)

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.exceptions import SendTaskError
from taskiq.flow import FlowProtocol
from taskiq.message import TaskiqMessage, _build_taskiq_message
from taskiq.router import TaskiqRoute, TaskiqRouter
from taskiq.scheduler.created_schedule import CreatedSchedule
from taskiq.scheduler.scheduled_task import CronSpec, ScheduledTask
from taskiq.task import AsyncTaskiqTask
from taskiq.utils import maybe_awaitable

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker
    from taskiq.abc.schedule_source import ScheduleSource

_T = TypeVar("_T")
_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")

logger = getLogger("taskiq")


class PreparedKiq(Generic[_ReturnType]):
    """Prepared task invocation that can be sent later."""

    def __init__(
        self,
        kicker: AsyncKicker[..., _ReturnType],
        message: TaskiqMessage,
        broker: AsyncBroker,
        route: TaskiqRoute | None,
        flow: FlowProtocol | None,
    ) -> None:
        self.kicker = kicker
        self.message = message
        self.broker = broker
        self.route = route
        self.flow = flow

    async def kiq(self) -> AsyncTaskiqTask[_ReturnType]:
        """Send prepared invocation."""
        return await self.kicker.kiq_message(
            self.message,
            broker=self.broker,
            route=self.route,
            flow=self.flow,
            use_current_route=False,
        )


class AsyncKicker(Generic[_FuncParams, _ReturnType]):
    """Class that used to modify data before sending it to broker."""

    def __init__(
        self,
        task_name: str,
        broker: AsyncBroker,
        labels: dict[str, Any],
        return_type: type[_ReturnType] | None = None,
    ) -> None:
        self.task_name = task_name
        self.broker = broker
        self.labels = labels
        self.custom_task_id: str | None = None
        self.custom_schedule_id: str | None = None
        self.return_type = return_type
        self.route: TaskiqRoute | None = None
        self.route_flow: FlowProtocol | None = None
        self._broker_overridden = False

    def with_labels(
        self,
        **labels: str | float,
    ) -> AsyncKicker[_FuncParams, _ReturnType]:
        """
        Update function's labels before sending.

        :param labels: new labels.
        :return: kicker with new labels.
        """
        self.labels.update(labels)
        return self

    def with_task_id(
        self,
        task_id: str | None,
    ) -> AsyncKicker[_FuncParams, _ReturnType]:
        """
        Set task_id for current execution.

        Please use this method with caution,
        because it may brake the logic of getting results.

        :param task_id: custom task id.
        :return: kicker with custom task id.
        """
        self.custom_task_id = task_id
        return self

    def with_schedule_id(
        self,
        schedule_id: str,
    ) -> AsyncKicker[_FuncParams, _ReturnType]:
        """
        Set schedule_id for current execution.

        :param schedule_id: custom schedule id.
        :return: kicker with custom schedule id.
        """
        self.custom_schedule_id = schedule_id
        return self

    def with_broker(
        self,
        broker: AsyncBroker,
    ) -> AsyncKicker[_FuncParams, _ReturnType]:
        """
        Replace broker for the function.

        This method can be used with
        shared tasks.

        :param broker: new broker instance.
        :return: Kicker with new broker.
        """
        self.broker = broker
        self.route = None
        self.route_flow = None
        self._broker_overridden = True
        return self

    def with_flow(
        self,
        flow: FlowProtocol | None,
    ) -> AsyncKicker[_FuncParams, _ReturnType]:
        """
        Replace flow for the current invocation.

        :param flow: flow to send message to.
        :return: Kicker with a route flow override.
        """
        if self.route is not None:
            self.route = replace(self.route, flow=flow)
        self.route_flow = flow
        return self

    def with_route(
        self,
        route: TaskiqRoute,
    ) -> AsyncKicker[_FuncParams, _ReturnType]:
        """
        Replace route for the current invocation.

        :param route: route to send message through.
        :return: Kicker with a route override.
        """
        self.route = route
        self.route_flow = route.flow
        return self

    def prepare(
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> PreparedKiq[_ReturnType]:
        """
        Prepare a task invocation without sending it.

        :param args: function's arguments.
        :param kwargs: function's key word arguments.
        :return: prepared task invocation.
        """
        broker, route, flow = self._prepare_route_snapshot()
        return PreparedKiq(
            self,
            self._prepare_message(*args, **kwargs),
            broker=broker,
            route=route,
            flow=flow,
        )

    @overload
    async def kiq(
        self: AsyncKicker[_FuncParams, CoroutineType[Any, Any, _T]],
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncTaskiqTask[_T]:  # pragma: no cover
        ...

    @overload
    async def kiq(
        self: AsyncKicker[_FuncParams, Coroutine[Any, Any, _T]],
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncTaskiqTask[_T]:  # pragma: no cover
        ...

    @overload
    async def kiq(
        self: AsyncKicker[_FuncParams, _ReturnType],
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncTaskiqTask[_ReturnType]:  # pragma: no cover
        ...

    async def kiq(
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> Any:
        """
        This method sends function call over the network.

        It gets current broker and calls it's kick method,
        returning what it returns.

        :param args: function's arguments.
        :param kwargs: function's key word arguments.

        :raises SendTaskError: if we can't send task to the broker.

        :returns: taskiq task.
        """
        logger.debug(
            f"Kicking {self.task_name} with args={args} and kwargs={kwargs}.",
        )
        return await self.kiq_message(self._prepare_message(*args, **kwargs))

    async def kiq_message(
        self,
        message: TaskiqMessage,
        *,
        broker: AsyncBroker | None = None,
        route: TaskiqRoute | None = None,
        flow: FlowProtocol | None = None,
        use_current_route: bool = True,
    ) -> AsyncTaskiqTask[_ReturnType]:
        """Send a prepared message."""
        try:
            target_broker = broker or self.broker
            if use_current_route:
                target_route = self.route if route is None else route
                target_flow = self.route_flow if flow is None else flow
            else:
                target_route = route
                target_flow = flow
            if target_route is not None:
                if broker is not None and broker is not target_route.broker:
                    raise ValueError("Pass either route or broker override.")
                target_broker = target_route.broker
                target_flow = None
            router = getattr(target_broker, "router", None)
            if isinstance(router, TaskiqRouter):
                broker_override = None
                if target_route is None and (
                    broker is not None or self._broker_overridden
                ):
                    broker_override = target_broker
                return await router.kiq(
                    message,
                    route=target_route,
                    broker=broker_override,
                    flow=target_flow,
                    return_type=self.return_type,
                )
            return await self._legacy_kiq(target_broker, message)
        except Exception as exc:
            raise SendTaskError from exc

    async def _legacy_kiq(
        self,
        broker: AsyncBroker,
        message: TaskiqMessage,
    ) -> AsyncTaskiqTask[_ReturnType]:
        """
        Send message through the pre-router broker path.

        This keeps middleware tests and external broker-like mocks compatible
        while real AsyncBroker instances use TaskiqRouter.
        """
        middlewares = getattr(broker, "middlewares", [])
        if not isinstance(middlewares, list):
            middlewares = []

        for middleware in middlewares:
            if middleware.__class__.pre_send != TaskiqMiddleware.pre_send:
                message = await maybe_awaitable(middleware.pre_send(message))

        await broker.kick(broker.formatter.dumps(message))

        for middleware in reversed(middlewares):
            if middleware.__class__.post_send != TaskiqMiddleware.post_send:
                await maybe_awaitable(middleware.post_send(message))

        return AsyncTaskiqTask(
            task_id=message.task_id,
            result_backend=cast(
                AsyncResultBackend[_ReturnType],
                broker.result_backend,
            ),
            return_type=self.return_type,
        )

    def _prepare_route_snapshot(
        self,
    ) -> tuple[AsyncBroker, TaskiqRoute | None, FlowProtocol | None]:
        """
        Resolve route state that a prepared invocation must keep.

        The returned tuple is `(broker, route, explicit_flow_override)`.
        When a route is present, the route already carries its flow, so the
        separate flow override is `None`. A separate flow is returned only for
        legacy broker paths that cannot snapshot a `TaskiqRoute`.
        """
        router = getattr(self.broker, "router", None)
        if not isinstance(router, TaskiqRouter):
            return self.broker, None, self.route_flow

        if self.route is not None:
            return self.route.broker, self.route, None

        broker_override = self.broker if self._broker_overridden else None
        route = router.resolve_route(
            self.task_name,
            broker=broker_override,
            flow=self.route_flow,
        )
        return route.broker, route, None

    async def schedule_by_cron(
        self,
        source: ScheduleSource,
        cron: str | CronSpec,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> CreatedSchedule[_ReturnType]:
        """
        Function to schedule task with cron.

        :param source: schedule source.
        :param cron: cron expression.
        :param args: function's args.
        :param kwargs: function's kwargs.

        :return: schedule id.
        """
        schedule_id = self.custom_schedule_id
        if schedule_id is None:
            schedule_id = self.broker.id_generator()
        message = self._prepare_message(*args, **kwargs)
        cron_offset = None
        if isinstance(cron, CronSpec):
            cron_str = cron.to_cron()
            cron_offset = cron.offset
        else:
            cron_str = cron
        scheduled = ScheduledTask(
            schedule_id=schedule_id,
            task_name=message.task_name,
            labels=message.labels,
            args=message.args,
            kwargs=message.kwargs,
            task_id=self.custom_task_id,
            cron=cron_str,
            cron_offset=cron_offset,
        )
        await source.add_schedule(scheduled)
        return CreatedSchedule(self, source, scheduled)

    async def schedule_by_interval(
        self,
        source: ScheduleSource,
        interval: int | timedelta,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> CreatedSchedule[_ReturnType]:
        """
        Function to schedule task using an interval.

        :param source: schedule source.
        :param interval: interval in seconds or timedelta instance.
        :param args: function's args.
        :param kwargs: function's kwargs.

        :return: schedule id.
        """
        schedule_id = self.custom_schedule_id
        if schedule_id is None:
            schedule_id = self.broker.id_generator()
        message = self._prepare_message(*args, **kwargs)
        scheduled = ScheduledTask(
            schedule_id=schedule_id,
            task_name=message.task_name,
            labels=message.labels,
            args=message.args,
            kwargs=message.kwargs,
            interval=interval,
        )
        await source.add_schedule(scheduled)
        return CreatedSchedule(self, source, scheduled)

    async def schedule_by_time(
        self,
        source: ScheduleSource,
        time: datetime,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> CreatedSchedule[_ReturnType]:
        """
        Function to schedule task to run at specific time.

        :param source: schedule source.
        :param time: time to run task at.
        :param args: function's args.
        :param kwargs: function's kwargs.
        """
        schedule_id = self.custom_schedule_id
        if schedule_id is None:
            schedule_id = self.broker.id_generator()
        message = self._prepare_message(*args, **kwargs)
        scheduled = ScheduledTask(
            schedule_id=schedule_id,
            task_name=message.task_name,
            labels=message.labels,
            args=message.args,
            kwargs=message.kwargs,
            task_id=self.custom_task_id,
            time=time,
        )
        await source.add_schedule(scheduled)
        return CreatedSchedule(self, source, scheduled)

    def _prepare_message(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> TaskiqMessage:
        """
        Create a message from args and kwargs.

        :param args: function's args.
        :param kwargs: function's kwargs.
        :return: constructed message.
        """
        task_id = self.custom_task_id
        if task_id is None:
            task_id = self.broker.id_generator()

        return _build_taskiq_message(
            task_id=task_id,
            task_name=self.task_name,
            labels=self.labels,
            args=args,
            kwargs=kwargs,
        )
