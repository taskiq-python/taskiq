import asyncio
from collections.abc import AsyncGenerator

from taskiq import AsyncBroker, BrokerMessage, FlowProtocol, TaskiqRouter
from taskiq.acks import AckableMessage


class RecordingBroker(AsyncBroker):
    """Broker for tests that records messages and selected flows."""

    def __init__(
        self,
        *,
        router: TaskiqRouter | None = None,
        broker_name: str | None = None,
        default_flow: FlowProtocol | None = None,
    ) -> None:
        self.sent: list[tuple[BrokerMessage, FlowProtocol | None]] = []
        super().__init__(
            router=router,
            broker_name=broker_name,
            default_flow=default_flow,
        )

    async def kick(self, message: BrokerMessage) -> None:
        """Record old-style send."""
        self.sent.append((message, None))

    async def kick_to_flow(
        self,
        message: BrokerMessage,
        flow: FlowProtocol | None = None,
    ) -> None:
        """Record flow-aware send."""
        self.sent.append((message, flow))

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """Recording broker does not listen in tests."""
        if False:
            yield b""


class AsyncQueueBroker(AsyncBroker):
    """
    Broker for testing.

    It simply puts all tasks in asyncio.Queue
    and returns them in listen method.
    """

    def __init__(self) -> None:
        self.queue: asyncio.Queue[bytes] = asyncio.Queue()
        super().__init__(None, None)

    async def kick(self, message: BrokerMessage) -> None:
        """Send a message to the queue."""
        await self.queue.put(message.message)

    async def wait_tasks(self) -> None:
        """Small method to wait for all tasks to be processed."""
        await self.queue.join()

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """This method returns all tasks from queue."""
        while True:
            task = await self.queue.get()
            yield AckableMessage(data=task, ack=self.queue.task_done)
