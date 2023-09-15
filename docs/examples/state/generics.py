from typing import Generic, TypeVar

from taskiq import TaskiqDepends

T = TypeVar("T")


def default_dependency() -> int:
    return 1


class GenericDep(Generic[T]):
    def __init__(self, dep: T = TaskiqDepends()) -> None:
        self.dep = dep

    def print_dep(self) -> None:
        print(self.dep)


class MyClassDep:
    def __init__(self, i: int = TaskiqDepends(default_dependency)) -> None:
        self.i = i


async def my_task(dep: GenericDep[MyClassDep] = TaskiqDepends()) -> None:
    assert isinstance(dep.dep, MyClassDep)
    dep.print_dep()
