import asyncio
from logging import getLogger
from typing import Any, Callable, Coroutine, Optional, TypeVar

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage

try:
    import zmq  # noqa: WPS433
    from zmq.asyncio import Context  # noqa: WPS433
except ImportError:
    zmq = None  # type: ignore

_T = TypeVar("_T")  # noqa: WPS111

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
        result_backend: "Optional[AsyncResultBackend[_T]]" = None,
        task_id_generator: Optional[Callable[[], str]] = None,
    ) -> None:
        if zmq is None:
            raise RuntimeError(
                "To use ZMQ broker please install pyzmq lib or taskiq[zmq].",
            )
        super().__init__(result_backend, task_id_generator)
        self.context = Context()
        self.pub_host = zmq_pub_host
        self.sub_host = zmq_sub_host
        if self.is_worker_process:
            self.socket = self.context.socket(zmq.SUB)
            self.socket.setsockopt(zmq.SUBSCRIBE, b"")
        else:
            self.socket = self.context.socket(zmq.PUB)
            self.socket.bind(self.pub_host)

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking message.

        This method is used to publish message
        via socket.

        :param message: message to publish.
        """
        with self.socket.connect(self.sub_host) as sock:
            await sock.send_string(message.json())

    async def listen(
        self,
        callback: Callable[[BrokerMessage], Coroutine[Any, Any, None]],
    ) -> None:
        """
        Start accepting new messages.

        :param callback: function to call when message received.
        """
        loop = asyncio.get_event_loop()
        with self.socket.connect(self.sub_host) as sock:
            while True:
                received_str = await sock.recv_string()
                try:
                    broker_msg = BrokerMessage.parse_raw(received_str)
                except ValueError:
                    logger.warning("Cannot parse received message %s", received_str)
                    continue
                loop.create_task(callback(broker_msg))
