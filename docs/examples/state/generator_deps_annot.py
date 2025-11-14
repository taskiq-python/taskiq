from collections.abc import Generator
from typing import Annotated

from taskiq import TaskiqDepends


def dependency() -> Generator[str, None, None]:
    print("Startup")

    yield "value"

    print("Shutdown")


async def my_task(dep: Annotated[str, TaskiqDepends(dependency)]) -> None:
    print(dep.upper())
