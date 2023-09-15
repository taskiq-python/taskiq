from typing import Annotated, Generator

from taskiq import TaskiqDepends


def dependency() -> Generator[str, None, None]:
    print("Startup")

    yield "value"

    print("Shutdown")


async def my_task(dep: Annotated[str, TaskiqDepends(dependency)]) -> None:
    print(dep.upper())
