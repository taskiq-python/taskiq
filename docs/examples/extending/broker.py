from typing import AsyncGenerator, Union

from taskiq import WrappedMessage, AsyncBroker, BrokerMessage


class MyBroker(AsyncBroker):
    def __init__(self) -> None:
        # Please call this super method to set default values to
        # many different fields.
        super().__init__()

    async def startup(self) -> None:
        # Here you can do some startup magic.
        # Like opening a connection.
        return await super().startup()

    async def shutdown(self) -> None:
        # Here you can perform shutdown operations.
        # Like closing connections.
        return await super().shutdown()

    async def kick(self, message: BrokerMessage) -> None:
        # Send a message.message.
        pass

    async def listen(self) -> AsyncGenerator[Union[bytes, WrappedMessage], None]:
        while True:
            # Get new message.
            new_message: bytes = ...  # type: ignore
            # Yield it!
            yield new_message
