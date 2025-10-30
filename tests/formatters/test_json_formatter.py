import json

from taskiq.formatters.json_formatter import JSONFormatter
from taskiq.message import BrokerMessage, TaskiqMessage


async def test_json_dumps() -> None:
    fmt = JSONFormatter()
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
            b'{"task_id":"task-id","task_name":"task.name",'
            b'"labels":{"label1":1,"label2":"text"},'
            b'"labels_types":null,'
            b'"args":[1,"a"],"kwargs":{"p1":"v1"}}'
        ),
        labels={"label1": 1, "label2": "text"},
    )
    dumped = fmt.dumps(msg)
    assert dumped.task_id == expected.task_id
    assert dumped.task_name == expected.task_name
    assert dumped.labels == expected.labels
    assert json.loads(dumped.message) == json.loads(expected.message)


async def test_json_loads() -> None:
    fmt = JSONFormatter()
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
    assert fmt.loads(msg) == expected
