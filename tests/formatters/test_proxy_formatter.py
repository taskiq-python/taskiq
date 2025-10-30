from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.message import BrokerMessage, TaskiqMessage


async def test_proxy_dumps() -> None:
    # uses json serializer by default
    broker = InMemoryBroker()
    msg = TaskiqMessage(
        task_id="task-id",
        task_name="task.name",
        labels={"label1": 1, "label2": "text"},
        args=[1, "a"],
        kwargs={"p1": "v1"},
    )
    expected = BrokerMessage(
        task_id="task-id",
        task_name="task.name",
        message=(
            b'{"task_id": "task-id", "task_name": "task.name", '
            b'"labels": {"label1": 1, "label2": "text"}, '
            b'"labels_types": null, '
            b'"args": [1, "a"], "kwargs": {"p1": "v1"}}'
        ),
        labels={"label1": 1, "label2": "text"},
    )
    assert broker.formatter.dumps(msg) == expected


async def test_proxy_loads() -> None:
    # uses json serializer by default
    broker = InMemoryBroker()
    msg = (
        b'{"task_id":"task-id","task_name":"task.name",'
        b'"labels":{"label1":1,"label2":"text"},'
        b'"args":[1,"a"],"kwargs":{"p1":"v1"}}'
    )
    expected = TaskiqMessage(
        task_id="task-id",
        task_name="task.name",
        labels={"label1": 1, "label2": "text"},
        args=[1, "a"],
        kwargs={"p1": "v1"},
    )
    assert broker.formatter.loads(msg) == expected
