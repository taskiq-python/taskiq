import asyncio
from typing import Any

from taskiq import InMemoryBroker

broker = InMemoryBroker()


@broker.task
async def divide(a: int, b: int) -> Any:
    return a / b


async def main() -> None:
    await broker.startup()

    task = await divide.kiq(1, 0)
    result = await task.wait_result()

    if result.is_err:
        print(f"Task failed with {type(result.error).__name__}: {result.error}")
    else:
        print(f"Returned value: {result.return_value}")

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
