from collections.abc import AsyncGenerator

import pynng

from taskiq.abc.broker import AsyncBroker
from taskiq.message import BrokerMessage


class NNGBroker(AsyncBroker):
    """
    NanoMSG next generation broker.

    This broker is very much alike to the ZMQ broker,
    It has a similar Idea, but slightly different
    implementation.
    """

    def __init__(self, addr: str) -> None:
        """
        Initialize the broker.

        :param addr: address which is used by both worker and client.
        """
        super().__init__()
        self.socket = pynng.Pair1(polyamorous=True)
        self.addr = addr

    async def startup(self) -> None:
        """Start the socket."""
        await super().startup()
        if self.is_worker_process:
            self.socket.listen(self.addr)
        else:
            self.socket.dial(self.addr, block=True)

    async def shutdown(self) -> None:
        """Close the socket."""
        await super().shutdown()
        self.socket.close()

    async def kick(self, message: BrokerMessage) -> None:
        """Send a message."""
        await self.socket.ascend(message.message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """Infinite loop that receives messages."""
        while True:
            yield await self.socket.arecv()
