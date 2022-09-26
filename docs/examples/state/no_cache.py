import random

from taskiq import TaskiqDepends


def common_dep() -> int:
    return random.randint(1, 10)


def dep1(cd: int = TaskiqDepends(common_dep)) -> int:
    return cd + 1


def dep2(cd: int = TaskiqDepends(common_dep, use_cache=False)) -> int:
    return cd + 2


def my_task(
    d1: int = TaskiqDepends(dep1),
    d2: int = TaskiqDepends(dep2),
) -> int:
    return d1 + d2
