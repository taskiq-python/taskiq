import asyncio
from typing import AsyncGenerator

from taskiq import AsyncBroker, BrokerMessage
from taskiq.acks import AckableMessage


class AsyncQueueBroker(AsyncBroker):
    """
    Broker for testing.

    It simply puts all tasks in asyncio.Queue
    and returns them in listen method.
    """

    def __init__(self) -> None:
        self.queue: "asyncio.Queue[bytes]" = asyncio.Queue()
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
