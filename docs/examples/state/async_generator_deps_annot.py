import asyncio
from typing import Annotated, AsyncGenerator

from taskiq import TaskiqDepends


async def dependency() -> AsyncGenerator[str, None]:
    print("Startup")
    await asyncio.sleep(0.1)

    yield "value"

    await asyncio.sleep(0.1)
    print("Shutdown")


async def my_task(dep: Annotated[str, TaskiqDepends(dependency)]) -> None:
    print(dep.upper())
