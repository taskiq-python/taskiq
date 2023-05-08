import abc
import asyncio
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, Optional

from taskiq.result import StreamableResult, TaskiqResult

if TYPE_CHECKING:
    from taskiq.abc.broker import AsyncBroker


class StreamableMessage:
    task_id: str
    result: bytes


class StreamableResultBackend(abc.ABC):
    def __init__(self) -> None:
        self.broker: "AsyncBroker" = None  # type: ignore
        self.result_map: Dict[str, StreamableResult] = {}
        self.listen_task: "Optional[asyncio.Task[Any]]" = None

    def set_broker(self, broker: "AsyncBroker") -> None:
        self.broker = broker

    async def _listen_loop(self) -> None:
        async for message in self.listen():
            try:
                res = TaskiqResult.parse_raw(message)
            except ValueError:
                continue
            self.set_result(res)

    def _create_task(self, *_: Any) -> None:
        task = asyncio.create_task(self._listen_loop())
        task.add_done_callback(self._create_task)
        self.listen_task = task

    async def startup(self) -> None:
        """
        Perform actions on startup.

        This function is used to initialize sender, or receiver.
        You can chceck whether your code is executing
        on clien or on worker, using `self.broker.is_worker_process`
        variable.
        """
        self._create_task()

    async def shutdown(self) -> None:
        """
        Perform actions on shutdown.

        This function is used to desctroy sender, or receiver.
        You can chceck whether your code is executing
        on clien or on worker, using `self.broker.is_worker_process`
        variable.
        """

    def set_result(self, result: "TaskiqResult[Any]") -> None:
        """
        This function sets results on client side.

        It checks that task_id is inside of the map,
        so we can check whether a person wants
        to await results. If result is not present
        in this map, it means that client has dropped
        the task object before we received a response.

        :param result: result to set.
        """
        if result.task_id not in self.result_map:
            return
        self.result_map[result.task_id].result = result
        self.result_map[result.task_id].readiness.set()

    @abc.abstractmethod
    def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Listen to new results.

        This function must listen to the queue
        and receive actual results from the queue.

        It does nothing, but yield results.
        """

    @abc.abstractmethod
    async def send_result(self, instance_id: str, result: bytes) -> None:
        """
        Send result to client.

        This function sends a computed resul over the queue
        to the client.

        :param instance_id: id of an instance to use when sending response.
        :param result: actual result.
        """
