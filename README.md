[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/taskiq?style=for-the-badge)](https://pypi.org/project/taskiq/)
[![PyPI](https://img.shields.io/pypi/v/taskiq?style=for-the-badge)](https://pypi.org/project/taskiq/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/taskiq?style=for-the-badge)](https://pypistats.org/packages/taskiq)

<div align="center">
<a href="https://taskiq-python.github.io/"><img src="https://raw.githubusercontent.com/taskiq-python/taskiq/master/imgs/logo.svg" width=600></a>
<hr/>
</div>

Documentation: https://taskiq-python.github.io/

## What is taskiq?

Taskiq is an asynchronous distributed task queue for python.
This project takes inspiration from big projects such as [Celery](https://docs.celeryq.dev) and [Dramatiq](https://dramatiq.io/).
But taskiq can send and run both the sync and async functions, has
integration with popular async frameworks, such as [FastAPI](https://fastapi.tiangolo.com/) and [AioHTTP](https://docs.aiohttp.org/en/stable/).

Also, we use [PEP-612](https://peps.python.org/pep-0612/) to provide the best autosuggestions possible. All code is type-hinted.

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

At first you need to create a broker. Broker is an object that can communicate to workers using distributed queues.

We have differet brokers for different queue backends. For example, we have a broker for [NATS](https://github.com/taskiq-python/taskiq-nats), [Redis](https://github.com/taskiq-python/taskiq-redis), [RabbitMQ](https://github.com/taskiq-python/taskiq-aio-pika), [Kafka](https://github.com/taskiq-python/taskiq-aio-kafka) and even more. Choose the one that fits you and create an instance.

```python
from taskiq_nats import JetStreamBroker

broker = JetStreamBroker("nats://localhost:4222", queue="my_queue")
```

Declaring tasks is as easy as declaring a function. Just add a decorator to your function and you are ready to go.

```python
import asyncio

from taskiq_nats import JetStreamBroker

broker = JetStreamBroker("nats://localhost:4222", queue="my_queue2")


@broker.task
async def my_task(a: int, b: int) -> None:
    print("AB", a + b)


async def main():
    await broker.startup()

    await my_task.kiq(1, 2)

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())


```

The message is going to be sent to the broker and then to the worker. The worker will execute the function. To start worker processes, just run the following command:

```bash
taskiq worker path.to.the.module:broker
```

Where `path.to.the.module` is the path to the module where the broker is defined and `broker` is the name of the broker variable.

If you have tasks in different modules, you can ask taskiq to automatically import them by passing the `--fs-discover` flag:

```bash
taskiq worker path.to.the.module:broker --fs-discover
```

It will import all modules called `tasks.py` in the current directory and all subdirectories.

Also, we support hot reload for workers. To enable it, just pass the `--reload` flag. It will reload the worker when the code changes (To use it, install taskiq with reload extra. E.g `pip install taskiq[reload]`).


Also, we have cool integrations with popular async frameworks. For example, we have an integration with [FastAPI](https://taskiq-python.github.io/framework_integrations/taskiq-with-fastapi.html) or [AioHTTP](https://taskiq-python.github.io/framework_integrations/taskiq-with-aiohttp.html). You can use it to reuse dependencies from your web app in your tasks.

Read about all features in our documentation: https://taskiq-python.github.io/

# Local development


## Linting

We use pre-commit to do linting locally.

After cloning this project, please install [pre-commit](https://pre-commit.com/#install). It helps fix files before committing changes.

```bash
pre-commit install
```


## Testing

Pytest can run without any additional actions or options.

```bash
pytest
```

## Docs

To run docs locally, you need to install [yarn](https://yarnpkg.com/getting-started/install).

First, you need to install dependencies.
```
yarn install
```

After that you can set up a docs server by running:

```
yarn docs:dev
```
