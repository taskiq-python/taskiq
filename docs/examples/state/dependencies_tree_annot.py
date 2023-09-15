import random
from typing import Annotated

from taskiq import TaskiqDepends


def common_dep() -> int:
    # For example it returns 8
    return random.randint(1, 10)


def dep1(cd: Annotated[int, TaskiqDepends(common_dep)]) -> int:
    # This function will return 9
    return cd + 1


def dep2(cd: Annotated[int, TaskiqDepends(common_dep)]) -> int:
    # This function will return 10
    return cd + 2


def my_task(
    d1: Annotated[int, TaskiqDepends(dep1)],
    d2: Annotated[int, TaskiqDepends(dep2)],
) -> int:
    # This function will return 19
    return d1 + d2
