from typing import Any

from opentelemetry import baggage

from taskiq import InMemoryBroker

broker = InMemoryBroker(
    await_inplace=True,  # used to sort spans in a deterministic way
)


class CustomError(Exception):
    pass


@broker.task
async def task_add(num_a: float, num_b: float) -> float:
    return num_a + num_b


@broker.task
async def task_raises() -> None:
    raise CustomError("The task failed!")


@broker.task
async def task_returns_baggage() -> Any:
    return dict(baggage.get_all())
