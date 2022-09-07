# broker.py
import asyncio

from taskiq_aio_pika import AioPikaBroker
from taskiq_redis import RedisAsyncResultBackend

broker = AioPikaBroker(
    "amqp://guest:guest@localhost:5672",
    result_backend=RedisAsyncResultBackend("redis://localhost"),
)


@broker.task
async def add_one(value: int) -> int:
    return value + 1


async def main() -> None:
    await broker.startup()
    # Send the task to the broker.
    task = await add_one.kiq(1)
    # Wait for the result.
    result = await task.wait_result(timeout=2)
    print(f"Task execution took: {result.execution_time} seconds.")
    if not result.is_err:
        print(f"Returned value: {result.return_value}")
    else:
        print("Error found while executing task.")


if __name__ == "__main__":
    asyncio.run(main())
