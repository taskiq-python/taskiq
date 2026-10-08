import uuid

from taskiq.brokers.shared_broker import AsyncSharedBroker


def test_shared_kicker_does_not_mutate_task_labels() -> None:
    shared_broker = AsyncSharedBroker()

    @shared_broker.task(task_name=uuid.uuid4().hex, original="value")
    async def task() -> None: ...

    task.kicker().with_labels(extra="label")

    assert task.labels == {"original": "value"}
