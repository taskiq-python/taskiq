# Taskiq

Taskiq is an asynchronous distributed task queue.
This project takes inspiration from big projects such as [Celery](https://docs.celeryq.dev) and [Dramatiq](https://dramatiq.io/).
But taskiq can send and run both the sync and async functions.
Also, we use [PEP-612](https://peps.python.org/pep-0612/) to provide the best autosuggestions possible. But since it's a new PEP, I encourage you to use taskiq with VS code because Pylance understands all types correctly.

# Installation

This project can be installed using pip:
```bash
pip install taskiq
```

Or it can be installed directly from git:

```bash
pip install git+https://github.com/taskiq-python/taskiq
```

# Usage

Let's see the example with the in-memory broker:

```python
import asyncio

from taskiq.brokers.inmemory_broker import InMemoryBroker


# This is broker that can be used for
# development or for demo purpose.
# In production environment consider using
# real distributed brokers, such as taskiq-aio-pika
# for rabbitmq.
broker = InMemoryBroker()


# Or you can optionally
# pass task_name as the parameter in the decorator.
@broker.task
async def my_async_task() -> None:
    """My lovely task."""
    await asyncio.sleep(1)
    print("Hello")


async def main():
    # Kiq is the method that actually
    # sends task over the network.
    task = await my_async_task.kiq()
    # Now we print result of execution.
    print(await task.get_result())


asyncio.run(main())

```


You can run it with python without any extra actions,
since this script uses the `InMemoryBroker`.

It won't send any data over the network,
and you cannot use this type of broker in
a real-world scenario, but it's useful for
local development if you do not want to
set up a taskiq worker.


## Brokers

Brokers are simple. They don't execute functions,
but they can send messages and listen to new messages.

Every broker implements the [taskiq.abc.broker.AsyncBroker](https://github.com/taskiq-python/taskiq/blob/master/taskiq/abc/broker.py#L50) abstract class. All tasks are assigned to brokers, so every time you call the `kiq` method, you send this task to the assigned broker. (This behavior can be changed, by using `Kicker` directly).

Also you can add middlewares to brokers using `add_middlewares` method.

Like this:

```python
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.middlewares.retry_middleware import SimpleRetryMiddleware

# This is broker that can be used for
# development or for demo purpose.
# In production environment consider using
# real distributed brokers, such as taskiq-aio-pika
# for rabbitmq.
broker = InMemoryBroker()
broker.add_middlewares(
    [
        SimpleRetryMiddleware(
            default_retry_count=4,
        )
    ]
)
```

To run middlewares properly you must add them using the `add_middlewares` method.
It lead to errors if you try to add them by modifying broker directly.

Also brokers have formatters. You can change format
of a message to be compitable with other task execution
systems, so your migration to taskiq can be smoother.

## Result backends

After task is complete it will try to save the results of execution
in result backends. By default brokers
use `DummyResultBackend` wich doesn't do anything. It
won't print the result in logs and it always returns
`None` as the `return_value`, and 0 for `execution_time`.
But some brokers can override it. For example `InMemoryBroker` by default uses `InMemoryResultBackend` and returns correct results.


## CLI

Taskiq has a command line interface to run workers.
It's very simple to get it to work.

You just have to provide path to your broker. As an example, if you want to start listen to new tasks
with broker in module `my_project.broker` you just
have to run:

```
taskiq my_project.broker:broker
```

taskiq can discover tasks modules to import,
if you add the `-fsd` (file system discover) option.

Let's assume we have project with the following structure:

```
test_project
├── broker.py
├── submodule
│   └── tasks.py
└── utils
    └── tasks.py
```

You can specify all tasks modules to import manually.

```bash
taskiq test_project.broker:broker test_projec.submodule.tasks test_projec.utils.tasks
```

Or you can let taskiq find all python modules named tasks in current directory recursively.

```bash
taskiq test_project.broker:broker -fsd
```

If you have uvloop installed, taskiq will automatically install new policies to event loop.

You can always run `--help` to see all possible options.


## Middlewares

Middlewares are used to modify message, or take
some actions after task is complete.

You can write your own middlewares by subclassing
the `taskiq.abc.middleware.TaskiqMiddleware`.

Every hook can be sync or async. Taskiq will execute it.

For example, this is a valid middleware.

```python
import asyncio

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage


class MyMiddleware(TaskiqMiddleware):
    async def pre_send(self, message: "TaskiqMessage") -> TaskiqMessage:
        await asyncio.sleep(1)
        message.labels["my_label"] = "my_value"
        return message

    def post_send(self, message: "TaskiqMessage") -> None:
        print(f"Message {message} was sent.")

```

You can use sync or async hooks without changing aything, but adding async to the hook signature.

Middlewares can store information in message.labels for
later use. For example `SimpleRetryMiddleware` uses labels
to remember number of failed attempts.

## Messages

Every message has labels. You can define labels
using `task` decorator, or you can add them using kicker.

For example:

```python

@broker.task(my_label=1, label2="something")
async def my_async_task() -> None:
    """My lovely task."""
    await asyncio.sleep(1)
    print("Hello")

async def main():
    await my_async_task.kiq()
```

It's equivalent to this

```python

@broker.task
async def my_async_task() -> None:
    """My lovely task."""
    await asyncio.sleep(1)
    print("Hello")

async def main():
    await my_async_task.kicker().with_labels(
        my_label=1,
        label2="something",
    ).kiq()
```

## Kicker

The kicker is the object that sends tasks.
When you call kiq it generates a Kicker instance,
remembering current broker and message labels.
You can change the labels you want to use for this particular task or you can even change broker.

For example:

```python
import asyncio

from taskiq.brokers.inmemory_broker import InMemoryBroker

broker = InMemoryBroker()
second_broker = InMemoryBroker()


@broker.task
async def my_async_task() -> None:
    """My lovely task."""
    await asyncio.sleep(1)
    print("Hello")


async def main():
    task = await my_async_task.kicker().with_broker(second_broker).kiq()
    print(await task.get_result())


asyncio.run(main())

```


# Available Brokers

Taskiq has several brokers out of the box:
* InMemoryBroker
* ZeroMQBroker

## InMemoryBroker

This broker is created for development purpose.
You can easily use it without setting up workers for your project.

It works the same as real brokers, but with some limitations.

1. It cannot use `pre_execute` and `post_execute` hooks in middlewares.
2. You cannot use it in multiprocessing applications without real result_backend.

This broker is sutable for local development only.

InMemoryBroker parameters:
* `sync_tasks_pool_size` - number of threads for threadpool executor.
    All sync functions are executed in threadpool.
* `logs_format` - format which is used to collect logs from task execution.
* `max_stored_results` - maximum number of results that is stored in memory. This number is used only if no custom result backend is provided.
* `cast_types` - whether to use agressive type cast for types.
* `result_backend` - custom result backend. By default
    it uses `InmemoryResultBackend`.
* `task_id_generator` - custom function to generate task ids.

## ZeroMQBroker

ZeroMQ is not available by default. To enable it. Please install [pyzmq](https://pypi.org/project/pyzmq/),
or you can install `taskiq[zmq]`.

This broker doesn't have limitations, but it requires you to set up a worker using taskiq CLI.
Also please ensure that worker process is started after your application is ready.

Because ZMQ publishes messages in socket and all worker processes will connect to it.

ZeroMQBroker parameters:
* `zmq_pub_host` - host that used to publish messages.
* `zmq_sub_host` - host to subscribe to publisher.
* `result_backend` - custom result backend.
* `task_id_generator` - custom function to generate task ids.
