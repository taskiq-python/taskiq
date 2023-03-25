---
order: 2
---

# Available brokers

In this section we'll list officially supported brokers.

## InMemoryBroker

This is a special broker for local development. It uses the same functions to execute tasks,
but all tasks are executed locally in the current thread.
By default it uses `InMemoryResultBackend` but this can be overriden.

## ZeroMQBroker

This broker uses [ZMQ](https://zeromq.org/) to communicate between worker and client processes.
It's suitable for small projects with only ONE worker process, because of the ZMQ architecture.

It publishes messages on the local port. All worker processes are reading messages from this port.
If you run many worker processes, all tasks will be executed `N` times, where `N` is the total number of worker processes.

::: danger Be careful!
If you choose this type of broker, please run taskiq with `-w 1` parameter,
otherwise you may encounter undefined behavior.
:::

To run this broker please install the [pyzmq](https://pypi.org/project/pyzmq/) lib. Or you can taskiq with `zmq` extra.

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

To kiq this task you have to options:

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


## Custom brokers

These brokers are not parts of the core taskiq lib. You can install them as a separate packages.

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
