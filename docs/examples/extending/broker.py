import asyncio
from typing import Any, Callable, Coroutine, Optional, TypeVar

from taskiq import AsyncBroker, AsyncResultBackend, BrokerMessage

_T = TypeVar("_T")


class MyBroker(AsyncBroker):
    def __init__(
        self,
        result_backend: "Optional[AsyncResultBackend[_T]]" = None,
        task_id_generator: Optional[Callable[[], str]] = None,
    ) -> None:
        # Please call this super and allow people to use their result_backends.
        super().__init__(result_backend, task_id_generator)

    async def startup(self) -> None:
        # Here you can do some startup magic.
        # Like opening a connection.
        return await super().startup()

    async def shutdown(self) -> None:
        # Here you can perform shutdown operations.
        # Like closing connections.
        return await super().shutdown()

    async def kick(self, message: BrokerMessage) -> None:
        # Send a message.
        pass

    async def listen(
        self,
        callback: Callable[[BrokerMessage], Coroutine[Any, Any, None]],
    ) -> None:
        loop = asyncio.get_event_loop()
        while True:
            # Get new message.
            # new_message = ...
            # Create a new task to execute.
            # loop.create_task(callback(new_message))
            pass
