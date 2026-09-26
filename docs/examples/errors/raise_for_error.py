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
    try:
        # Raises ZeroDivisionError, otherwise returns the result itself.
        result = (await task.wait_result()).raise_for_error()
    except ZeroDivisionError:
        print("Cannot divide by zero.")
    else:
        print(f"Returned value: {result.return_value}")

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
