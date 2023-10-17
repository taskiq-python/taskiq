---
order: 11
---

# Taskiq message format

Taskiq doesn't force you to use any specific message format. We define default message format,
but you can use any format you want.

The default message format is:


::: tabs

@tab example

```json
{
    "task_name": "my_project.module1.task",
    "args": [1, 2, 3],
    "kwargs": {"a": 1, "b": 2, "c": 3},
    "labels": {
        "label1": "value1",
        "label2": "value2"
    }
}
```

@tab json schema

```json
{
  "properties": {
    "task_id": {
      "title": "Task Id",
      "type": "string"
    },
    "task_name": {
      "title": "Name of the task",
      "type": "string"
    },
    "labels": {
      "title": "Additional labels",
      "type": "object"
    },
    "args": {
      "items": {},
      "title": "Arguments",
      "type": "array"
    },
    "kwargs": {
      "title": "Keyword arguments",
      "type": "object"
    }
  },
  "required": [
    "task_id",
    "task_name",
    "labels",
    "args",
    "kwargs"
  ],
  "type": "object"
}
```

:::

But this can be easily changed by creating your own implementation of the TaskiqFormatter class or TaskiqSerializer class.


### Serializers

Serializers define the format of the message but not the structure. For example, if you want to use msgpack or ORJson to serialize your message, you should update the serializer of your broker.

Be default, Taskiq uses JSON serializer. But we also have some implementations of other serializers:

* ORJSONSerializer - faster [JSON implementation](https://pypi.org/project/orjson/). Also, it supports datetime and UUID serialization.
* MSGPackSerializer - [MsgPack](https://pypi.org/project/msgpack/) format serializer. It might be useful to send less data over the network.
* CBORSerializer - [CBOR](https://pypi.org/project/cbor2/) format serializer. It is also has a smaller size than JSON.

To define your own serializer, you have to subclass the TaskiqSerializer class and implement `dumpb` and `loadb` methods. You can take a look at the existing implementations from the `taskiq.serializers` module.

To install taskiq with libraries for non-JSON serializers, you should install taskiq with extras.

::: tabs

@tab orjson

```bash
pip install "taskiq[orjson]"
```

@tab msgpack

```bash
pip install "taskiq[msgpack]"
```

@tab cbor

```bash
pip install "taskiq[cbor]"
```

:::

### Formatters

Formatters define the format of the message. It might be useful if you'd like to send a task to a celery worker for a different project. You can do it in seriazier as well, but formatters give you correct type hints.

By default we use a formatter that dumps the message to dict and serializes it using serializer. But you can define your own formatter to send a message in any format you want. To define a new formatter, you have to subclass the TaskiqFormatter class and implement `dumps` and `loads` methods.
As an example, you can take a look at the `JSONFormatter` from `taskiq.formatters` implementation.
