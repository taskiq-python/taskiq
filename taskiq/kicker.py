from dataclasses import asdict, is_dataclass
from logging import getLogger
from typing import (  # noqa: WPS235
    TYPE_CHECKING,
    Any,
    Coroutine,
    Dict,
    Generic,
    Optional,
    TypeVar,
    Union,
    overload,
)

from pydantic import BaseModel
from typing_extensions import ParamSpec

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.exceptions import SendTaskError
from taskiq.message import TaskiqMessage
from taskiq.task import AsyncTaskiqTask
from taskiq.utils import maybe_awaitable

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker

_T = TypeVar("_T")  # noqa: WPS111
_FuncParams = ParamSpec("_FuncParams")
_ReturnType = TypeVar("_ReturnType")

logger = getLogger("taskiq")


class AsyncKicker(Generic[_FuncParams, _ReturnType]):
    """Class that used to modify data before sending it to broker."""

    def __init__(
        self,
        task_name: str,
        broker: "AsyncBroker",
        labels: Dict[str, Any],
    ) -> None:
        self.task_name = task_name
        self.broker = broker
        self.labels = labels
        self.custom_task_id: Optional[str] = None

    def with_labels(
        self,
        **labels: Union[str, int, float],
    ) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Update function's labels before sending.

        :param labels: new labels.
        :return: kicker with new labels.
        """
        self.labels.update(labels)
        return self

    def with_task_id(self, task_id: str) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Set task_id for current execution.

        Please use this method with caution,
        because it may brake the logic of getting results.

        :param task_id: custom task id.
        :return: kicker with custom task id.
        """
        self.custom_task_id = task_id
        return self

    def with_broker(
        self,
        broker: "AsyncBroker",
    ) -> "AsyncKicker[_FuncParams, _ReturnType]":
        """
        Replace broker for the function.

        This method can be used with
        shared tasks.

        :param broker: new broker instance.
        :return: Kicker with new broker.
        """
        self.broker = broker
        return self

    @overload
    async def kiq(  # noqa: D102
        self: "AsyncKicker[_FuncParams, Coroutine[Any, Any, _T]]",
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncTaskiqTask[_T]:  # pragma: no cover
        ...

    @overload
    async def kiq(  # noqa: D102
        self: "AsyncKicker[_FuncParams, _ReturnType]",
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncTaskiqTask[_ReturnType]:  # pragma: no cover
        ...

    async def kiq(  # noqa: C901
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
        message = self._prepare_message(*args, **kwargs)
        for middleware in self.broker.middlewares:
            if middleware.__class__.pre_send != TaskiqMiddleware.pre_send:
                message = await maybe_awaitable(middleware.pre_send(message))
        try:
            await self.broker.kick(self.broker.formatter.dumps(message))
        except Exception as exc:
            raise SendTaskError() from exc

        for middleware in self.broker.middlewares:
            if middleware.__class__.post_send != TaskiqMiddleware.post_send:
                await maybe_awaitable(middleware.post_send(message))

        return AsyncTaskiqTask(
            task_id=message.task_id,
            result_backend=self.broker.result_backend,
        )

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
            arg = arg.dict()
        if is_dataclass(arg):
            arg = asdict(arg)
        return arg

    def _prepare_message(  # noqa: WPS210
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
        for arg in args:
            formatted_args.append(self._prepare_arg(arg))
        for kwarg_name, kwarg_val in kwargs.items():
            formatted_kwargs[kwarg_name] = self._prepare_arg(kwarg_val)
        for label, label_val in self.labels.items():
            labels[label] = str(label_val)

        task_id = self.custom_task_id
        if task_id is None:
            task_id = self.broker.id_generator()

        return TaskiqMessage(
            task_id=task_id,
            task_name=self.task_name,
            labels=labels,
            args=formatted_args,
            kwargs=formatted_kwargs,
        )
