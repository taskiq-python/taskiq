# broker.py
import asyncio

from taskiq import InMemoryBroker

broker = InMemoryBroker()


@broker.task(batch=True, batch_size=100, batch_timeout=3)
async def process_items(items: list[int]) -> int:
    # The worker collects many `.kiq` calls and invokes this
    # function once with the accumulated list of items.
    print(f"Processing a batch of {len(items)} items.")
    return sum(items)


async def main() -> None:
    await broker.startup()
    # Each `.kiq` sends a single item. They are buffered and run
    # together once the batch is flushed.
    tasks = [await process_items.kiq(i) for i in range(10)]
    # In tests, `wait_all` flushes any pending batches and waits
    # for them to finish before we read the results.
    await broker.wait_all()
    for task in tasks:
        result = await task.wait_result(timeout=5)
        # Every item in the batch shares the same batch result.
        print(f"Returned value: {result.return_value}")
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
