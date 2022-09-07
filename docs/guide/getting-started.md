---
title: Getting started
order: 2
---

# Getting started


## Installation

You can install taskiq from pypi or directly from git using pip:


::: tabs

@tab pypi
```bash:no-line-numbers
pip install taskiq
```

@tab git
```bash:no-line-numbers
pip install git+https://github.com/taskiq-python/taskiq.git
```

:::


After installation of the core library, you need to find the broker that fits you. You can do it using [PyPI](https://pypi.org/search/?q=taskiq) search.

::: info Cool tip!

We highly recommend [taskiq-aio-pika](https://pypi.org/project/taskiq-aio-pika/) as the broker and [taskiq-redis](https://pypi.org/project/taskiq-redis/) as the result backend for production use.

:::

## Running tasks

Now you need to create a python module with broker declaration. It's just a plain python file with the variable of your broker. For this particular example, I'm going to use the `InMemoryBroker`.

::: danger Important note
The InMemoryBroker doesn't send any data over the network,
and you cannot use this broker in
a real-world scenario, but it's still useful for
local development if you do not want to
set up a taskiq worker.

:::

```python
# broker.py
from taskiq import InMemoryBroker

broker = InMemoryBroker()
```

And that's it. Now let's add some tasks and the main function. You can add tasks in separate modules. You can find more information about that further.

@[code python](../examples/introduction/inmemory_run.py)

If you run this code, you will get this in your terminal:

```bash:no-line-numbers
❯ python mybroker.py
Task execution took: 7.3909759521484375e-06 seconds.
Returned value: 2
```

Ok, the code of the task execution is a little bit fancier than an ordinary function call, but it's still relatively simple to understand. To send a task to the broker,
you need to call the `.kiq` method on the function,
it returns the `TaskiqTask` object that can check whether the result is ready
or it can wait for it to become available.

You can get more information about taskiq types, CLI and internal structure in the "[Architecture overview](./architecture-overview.md)" section.

## Distributed run

Now let's change InMemoryBroker to some distributed broker instead. In this example we are going to use
broker that works with rabbitMQ.

At first we must install the [taskiq-aio-pika](https://pypi.org/project/taskiq-aio-pika/) lib.

```bash:no-line-numbers
pip install taskiq-aio-pika
```

After the installation, replace the broker we defined earlier with the broker from the `taskiq-aio-pika`.

```python
from taskiq_aio_pika import AioPikaBroker

broker = AioPikaBroker('amqp://guest:guest@localhost:5672')
```
Also, AioPika broker requires to call startup before using it. Add this line at the beginning of the
main function.

```python
await broker.startup()
```

That's all you need to do.

::: details Complete code

@[code python](../examples/introduction/aio_pika_broker.py)

:::


Let's run the worker process. First of all, we need rabbitMQ up and running. I highly recommend you use docker.

::: tabs


@tab linux|macos
```bash
docker run --rm -d \
    -p "5672:5672" \
    -p "15672:15672" \
    --env "RABBITMQ_DEFAULT_USER=guest" \
    --env "RABBITMQ_DEFAULT_PASS=guest" \
    --env "RABBITMQ_DEFAULT_VHOST=/" \
    rabbitmq:3.8.27-management-alpine
```

@tab windows

```powershell
docker run --rm -d ^
    -p "5672:5672" ^
    -p "15672:15672" ^
    --env "RABBITMQ_DEFAULT_USER=guest" ^
    --env "RABBITMQ_DEFAULT_PASS=guest" ^
    --env "RABBITMQ_DEFAULT_VHOST=/" ^
    rabbitmq:3.8.27-management-alpine
```


:::


Now we need to start worker process by running taskiq cli command. You can get more info about the CLI in the [CLI](./cli.md) section.

```bash:no-line-numbers
taskiq broker:broker
```

After the worker is up, we can run our script as an ordinary python file and see how the worker executes tasks.

```bash:no-line-numbers
$ python broker.py
Task execution took: 0.0 seconds.
Returned value: None
```

But the printed result value is not correct. That happens because we didn't provide any result backend that can store results
of task execution.
To store results, we can use the [taskiq-redis](https://pypi.org/project/taskiq-redis/) library.

```bash
pip install taskiq-redis
```

After the installation, add a new result backend to the broker.

```python
from taskiq_redis import RedisAsyncResultBackend

broker = AioPikaBroker(
    "amqp://guest:guest@localhost:5672",
    result_backend=RedisAsyncResultBackend("redis://localhost"),
)
```

Now we need to start redis.

::: tabs


@tab linux|macos
```bash
docker run --rm -d \
    -p "6379:6379" \
    redis
```

@tab windows

```powershell
docker run --rm -d ^
    -p "6379:6379" ^
    redis
```


:::


::: details Complete code

@[code python](../examples/introduction/full_example.py)

:::


Let's run taskiq once again. The command is the same.

```bash:no-line-numbers
taskiq broker:broker
```

Now, if we run this file with python, we can get the correct results with a valid execution time.

```bash
$ python broker.py
Task execution took: 1.0013580322265625e-05 seconds.
Returned value: 2
```

Continue reading to get more information about taskiq internals.
