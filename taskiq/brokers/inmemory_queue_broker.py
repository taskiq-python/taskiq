import asyncio
from typing import AsyncGenerator

from taskiq import BrokerMessage
from taskiq.brokers.inmemory_broker import InMemoryBroker


class InMemoryQueueBroker(InMemoryBroker):
    """In memory Broker based on asyncio.Queue."""

    def __init__(
        self,
        sync_tasks_pool_size: int = 4,
        max_stored_results: int = 100,
        cast_types: bool = True,
        max_async_tasks: int = 30,
    ) -> None:
        super().__init__(
            sync_tasks_pool_size,
            max_stored_results,
            cast_types,
            max_async_tasks,
        )
        self.queue: asyncio.Queue[BrokerMessage] = asyncio.Queue()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking task.

        :param message: incoming message
        """
        await self.queue.put(message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Listening for messages.

        :yields: message's raw data
        """
        running = asyncio.create_task(self.running.wait())

        while not self.running.is_set():
            message = asyncio.create_task(self.queue.get())
            await asyncio.wait(
                [running, message],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if message.done():
                yield (await message).message
                continue

            message.cancel()
            await running

    async def startup(self) -> None:
        """Runs startup events for client and worker side."""
        await super().startup()
        self.running = asyncio.Event()

    async def shutdown(self) -> None:
        """Runs shutdown events for client and worker side."""
        await super().shutdown()
        self.running.set()
