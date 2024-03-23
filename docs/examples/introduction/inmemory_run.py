# broker.py
import asyncio
import logging

from taskiq import InMemoryBroker
from taskiq.task import AsyncTaskiqTask

broker = InMemoryBroker()
task_logger = logging.getLogger("taskiq.tasklogger")


@broker.task
async def add_one(value: int) -> int:
    task_logger.info(f"Adding 1 to {value}")
    return value + 1


async def main() -> None:
    # Never forget to call startup in the beginning.
    await broker.startup()
    # Send the task to the broker.
    task = await add_one.kiq(1)
    # Wait for the result.
    result = await task.wait_result(with_logs=True)
    print(f"Task execution took: {result.execution_time} seconds.")
    if not result.is_err:
        print(f"Returned value: {result.return_value}")
        print(f"Logs: {result.log}")
    else:
        print("Error found while executing task.")
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
