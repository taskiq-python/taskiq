from __future__ import annotations

from collections.abc import Coroutine
from dataclasses import asdict, is_dataclass
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

from pydantic import BaseModel

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.compat import model_dump
from taskiq.exceptions import SendTaskError
from taskiq.flow import Flow
from taskiq.labels import prepare_label
from taskiq.message import TaskiqMessage
from taskiq.router import TaskiqRouter
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
    ) -> None:
        self.kicker = kicker
        self.message = message

    async def kiq(self) -> AsyncTaskiqTask[_ReturnType]:
        """Send prepared invocation."""
        return await self.kicker.kiq_message(self.message)


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
        self.route_broker: AsyncBroker | str | None = None
        self.route_flow: Flow | None = None

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
        self.route_broker = broker
        return self

    def with_flow(
        self,
        flow: Flow | None,
    ) -> AsyncKicker[_FuncParams, _ReturnType]:
        """
        Replace flow for the current invocation.

        :param flow: flow to send message to.
        :return: Kicker with a route flow override.
        """
        self.route_flow = flow
        return self

    def with_route(
        self,
        broker: AsyncBroker | str,
        flow: Flow | None,
    ) -> AsyncKicker[_FuncParams, _ReturnType]:
        """
        Replace broker and flow for the current invocation.

        :param broker: broker instance or broker name.
        :param flow: flow to send message to.
        :return: Kicker with a route override.
        """
        self.route_broker = broker
        self.route_flow = flow
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
        return PreparedKiq(self, self._prepare_message(*args, **kwargs))

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
    ) -> AsyncTaskiqTask[_ReturnType]:
        """Send a prepared message."""
        try:
            router = getattr(self.broker, "router", None)
            if isinstance(router, TaskiqRouter):
                return await router.kiq(
                    message,
                    broker=self.route_broker,
                    flow=self.route_flow,
                    return_type=self.return_type,
                )
            return await self._legacy_kiq(message)
        except Exception as exc:
            raise SendTaskError from exc

    async def _legacy_kiq(self, message: TaskiqMessage) -> AsyncTaskiqTask[_ReturnType]:
        """
        Send message through the pre-router broker path.

        This keeps middleware tests and external broker-like mocks compatible
        while real AsyncBroker instances use TaskiqRouter.
        """
        middlewares = getattr(self.broker, "middlewares", [])
        if not isinstance(middlewares, list):
            middlewares = []

        for middleware in middlewares:
            if middleware.__class__.pre_send != TaskiqMiddleware.pre_send:
                message = await maybe_awaitable(middleware.pre_send(message))

        await self.broker.kick(self.broker.formatter.dumps(message))

        for middleware in reversed(middlewares):
            if middleware.__class__.post_send != TaskiqMiddleware.post_send:
                await maybe_awaitable(middleware.post_send(message))

        return AsyncTaskiqTask(
            task_id=message.task_id,
            result_backend=cast(
                AsyncResultBackend[_ReturnType],
                self.broker.result_backend,
            ),
            return_type=self.return_type,
        )

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

    @classmethod
    def _prepare_arg(cls, arg: Any) -> Any:
        """
        Parses argument if possible.

        This function is used to construct dicts
        from pydantic models or dataclasses.

        :param arg: argument to format.
        :return: Formatted argument.
        """
        if isinstance(arg, BaseModel):
            arg = model_dump(arg)
        if is_dataclass(arg):
            if isinstance(arg, type):
                raise ValueError(
                    f"Cannot serialize types. The {arg} is not serializable.",
                )
            arg = asdict(arg)
        return arg

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
        formatted_args = []
        formatted_kwargs = {}
        labels = {}
        labels_types = {}
        for arg in args:
            formatted_args.append(self._prepare_arg(arg))
        for kwarg_name, kwarg_val in kwargs.items():
            formatted_kwargs[kwarg_name] = self._prepare_arg(kwarg_val)

        for label, label_val in self.labels.items():
            labels[label], labels_types[label] = prepare_label(label_val)

        task_id = self.custom_task_id
        if task_id is None:
            task_id = self.broker.id_generator()

        return TaskiqMessage(
            task_id=task_id,
            task_name=self.task_name,
            labels=labels,
            labels_types=labels_types,
            args=formatted_args,
            kwargs=formatted_kwargs,
        )
