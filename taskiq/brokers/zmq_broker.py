import math
from collections.abc import AsyncGenerator, Callable
from logging import getLogger
from typing import TypeVar

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage

try:
    import zmq
    from zmq.asyncio import Context, Socket
except ImportError:
    zmq = None  # type: ignore

_T = TypeVar("_T")

logger = getLogger(__name__)


class ZeroMQBroker(AsyncBroker):
    """
    ZeroMQ broker.

    This broker starts a socket ON A CLIENT SIDE,
    and all workers connect to this socket using sub_host.

    If you're using this socket you have to be sure,
    that your workers start after the client is ready.
    """

    def __init__(
        self,
        zmq_pub_host: str = "tcp://0.0.0.0:5555",
        zmq_sub_host: str = "tcp://localhost:5555",
        result_backend: "AsyncResultBackend[_T] | None" = None,
        task_id_generator: Callable[[], str] | None = None,
    ) -> None:
        if zmq is None:
            raise RuntimeError(
                "To use ZMQ broker please install pyzmq lib or taskiq[zmq].",
            )
        super().__init__(result_backend, task_id_generator)
        self.context = Context()
        self.pub_host = zmq_pub_host
        self.sub_host = zmq_sub_host
        self.socket: Socket

    async def startup(self) -> None:
        """
        Startup for zmq broker.

        This function creates actual connections to
        sockets. if current process is worker,
        it subscribes, otherwise it becomes publisher.
        """
        if self.is_worker_process:
            self.socket = self.context.socket(zmq.SUB)
            self.socket.setsockopt(zmq.SUBSCRIBE, b"")
        else:
            self.socket = self.context.socket(zmq.PUB)
            self.socket.bind(self.pub_host)
        await super().startup()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking message.

        This method is used to publish message
        via socket.

        :param message: message to publish.
        """
        part_len = 100
        parts = [
            message.message[
                idx * part_len : min(idx * part_len + part_len, len(message.message))
            ]
            for idx in range(math.ceil(len(message.message) / part_len))
        ]
        with self.socket.connect(self.pub_host) as sock:
            await sock.send_multipart(parts)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Start accepting new messages.

        :yields: incoming messages.
        """
        with self.socket.connect(self.sub_host) as sock:
            while True:
                data = await sock.recv_multipart()
                yield b"".join(data)
