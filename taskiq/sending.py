from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, TypeVar, cast

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.exceptions import SendTaskError
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.task import AsyncTaskiqTask
from taskiq.utils import maybe_awaitable

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker

_ReturnType = TypeVar("_ReturnType")
_TransportSend = Callable[[BrokerMessage], Awaitable[None]]


async def send_task_message(
    broker: "AsyncBroker",
    message: TaskiqMessage,
    *,
    send: _TransportSend,
    return_type: type[_ReturnType] | None = None,
) -> AsyncTaskiqTask[_ReturnType]:
    """
    Run the client-side send lifecycle through one broker.

    Middleware failures keep their original types. Only message formatting and
    transport dispatch failures are mapped to `SendTaskError`, matching the
    established Taskiq kicker contract.
    """
    middlewares = getattr(broker, "middlewares", [])
    if not isinstance(middlewares, list):
        middlewares = []

    for middleware in middlewares:
        if middleware.__class__.pre_send != TaskiqMiddleware.pre_send:
            message = await maybe_awaitable(middleware.pre_send(message))

    try:
        broker_message = broker.formatter.dumps(message)
        await send(broker_message)
    except Exception as exc:
        raise SendTaskError from exc

    for middleware in reversed(middlewares):
        if middleware.__class__.post_send != TaskiqMiddleware.post_send:
            await maybe_awaitable(middleware.post_send(message))

    return AsyncTaskiqTask(
        task_id=message.task_id,
        result_backend=cast(
            AsyncResultBackend[_ReturnType],
            broker.result_backend,
        ),
        return_type=return_type,
    )
