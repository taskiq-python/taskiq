---
order: 2
---

# Available brokers

In this section we'll list officially supported brokers.

## InMemoryBroker

This is a special broker for local development. It uses the same functions to execute tasks,
but all tasks are executed locally in the current thread.
By default it uses `InMemoryResultBackend` but this can be overridden.

## ZeroMQBroker

This broker uses [ZMQ](https://zeromq.org/) to communicate between worker and client processes.
It's suitable for small projects with only ONE worker process, because of the ZMQ architecture.

It publishes messages on the local port. All worker processes are reading messages from this port.
If you run many worker processes, all tasks will be executed `N` times, where `N` is the total number of worker processes.

::: caution Be careful!
If you choose this type of broker, please run taskiq with `-w 1` parameter,
otherwise you may encounter undefined behavior.
:::

To run this broker please install the [pyzmq](https://pypi.org/project/pyzmq/) lib. Or you can install taskiq with `zmq` extra.

::: tabs

@tab Only PyZMQ

```bash
pip install pyzmq
```

@tab Taskiq with ZMQ

```bash
pip install "taskiq[zmq]"
```

:::

## Async shared broker and shared tasks

This is also a special broker. You cannot use it directly. It's used to create shared tasks.
These tasks can be imported along with user defined tasks. To define a shared task please use this broker.

```python
from taskiq.brokers.shared_broker import async_shared_broker

@async_shared_broker.task
def my_task() -> bool:
    return True
```

To kiq this task you have two options:

- Explicitly define broker using kicker for this kiq;
- Add default broker for all shared tasks.

::: tabs

@tab Defining default broker

```python
from taskiq.brokers.shared_broker import async_shared_broker

async_shared_broker.default_broker(broker)
```

@tab using kicker

```python
await my_task.kicker().with_broker(broker).kiq()
```

:::


## MultiBroker

This broker combines multiple real brokers into one, so a single worker
can consume messages from all of them at the same time.
Every inner broker keeps its own connection and queue. The `MultiBroker`
only merges the listening and routes kicks.

This is useful when you want multiple queues but a single worker deployment
that processes all of them.

Tasks can be registered on inner brokers directly:

```python
from taskiq import MultiBroker
from taskiq_aio_pika import AioPikaBroker, Queue

fast_broker = AioPikaBroker(
    "amqp://guest:guest@localhost:5672/",
    task_queues=[Queue(name="fast_tasks")],
)
slow_broker = AioPikaBroker(
    "amqp://guest:guest@localhost:5672/",
    task_queues=[Queue(name="slow_tasks")],
)
broker = MultiBroker(fast_broker, slow_broker)
```

```python
# tasks.py
from my_project.broker import fast_broker, slow_broker

@fast_broker.task
async def fast_task() -> None:
    ...

@slow_broker.task
async def slow_task() -> None:
    ...
```

With this setup a single worker command consumes messages from both queues:

```bash
taskiq worker my_project.broker:broker my_project.tasks
```

It's also possible to register tasks on the `MultiBroker` itself.
In this case pass a `routing` mapping and mark tasks with the `queue_name` label:

```python
broker = MultiBroker(
    fast_broker,
    slow_broker,
    routing={"fast": fast_broker, "slow": slow_broker},
)

@broker.task(queue_name="slow")
async def slow_task() -> None:
    ...
```

Messages without the routing label are kicked using the default broker
(the first one, unless `default_broker` is passed).

If one of the inner brokers fails (for example, its cluster is down),
it's excluded from listening: the error is logged and the worker keeps
consuming messages from the remaining brokers. Only when every inner
broker is down does the worker process stop. Since the default
`--max-fails` value is `-1`, the supervisor restarts the worker
indefinitely, so it picks the brokers back up as soon as they recover.

::: caution Be careful!
The worker side uses formatters and serializers of the `MultiBroker`,
while kicks go through inner brokers. Default settings are compatible,
but if you customize them, make sure they match on both sides.

Also, middlewares of inner brokers work only on the kick side,
and middlewares of the `MultiBroker` — only on the execution side.

Use `MultiBroker.with_result_backend()` to set a result backend:
it propagates the backend to all inner brokers, keeping client-side
polling and worker-side saving in sync.
:::

If all of your queues live in a single RabbitMQ broker, prefer the native
multi-queue support of [taskiq-aio-pika](https://github.com/taskiq-python/taskiq-aio-pika)
(it listens to all `task_queues` over a single connection).
The `MultiBroker` is useful when queues are served by different brokers,
or by brokers without native multi-queue support. Note that it opens
one connection per inner broker in every worker process.


## Custom brokers

These brokers are not part of the core Taskiq lib. But they are maintained by Taskiq developers.
You can install them as separate packages.

You can read more about parameters and abilities of these brokers in README.md of each repo.


###  AioPikaBroker (for RabbitMQ)

Project link: [taskiq-aio-pika](https://github.com/taskiq-python/taskiq-aio-pika).

```bash
pip install taskiq-aio-pika
```

### Redis broker

Project link: [taskiq-redis](https://github.com/taskiq-python/taskiq-redis).

```bash
pip install taskiq-redis
```

### NATS broker

Project link: [taskiq-nats](https://github.com/taskiq-python/taskiq-nats).

```bash
pip install taskiq-nats
```

### SQS broker

Project link: [taskiq-sqs](https://github.com/taskiq-python/taskiq-sqs).

```bash
pip install taskiq-sqs
```

### Kafka broker

Project link: [taskiq-aio-kafka](https://github.com/taskiq-python/taskiq-aio-kafka).

```bash
pip install taskiq-aio-kafka
```

## Third-party brokers

These brokers are not part of the core Taskiq library. They are maintained by other open‑source contributors. You can install them as separate packages.

You can read more about parameters and abilities of these brokers in README.md of each repo.

### PostgreSQL broker

Project link: [taskiq-postgresql](https://github.com/z22092/taskiq-postgresql).

```bash
pip install taskiq-postgresql
```

### SQS broker

Project link: [taskiq-aio-sqs](https://github.com/vonsteer/taskiq-aio-sqs).

```bash
pip install taskiq-aio-sqs
```

### YDB broker

Project link: [taskiq-ydb](https://github.com/danfimov/taskiq-ydb).

```bash
pip install taskiq-ydb
```

### Azure Service Bus broker

Project link: [taskiq-service-bus](https://github.com/hamiltonkibbe/taskiq-service-bus).

```bash
pip install taskiq-service-bus
```
