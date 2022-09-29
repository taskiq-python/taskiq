from typing import Generator

from taskiq import TaskiqDepends


def dependency() -> Generator[str, None, None]:
    print("Startup")

    yield "value"

    print("Shutdown")


async def my_task(dep: str = TaskiqDepends(dependency)) -> None:
    print(dep.upper())
