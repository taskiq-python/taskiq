---
order: 10
---

# Taskiq + FastAPI

FastAPI is one of the most popular async web frameworks in python. It has gained it's popularity because of two things:
1. It's easy to use;
2. It has a cool dependency injection.

In taskiq we try to make our libraries easy to use and we too have a depenndency injection. But our dependencies
are not compatible with FastAPI's dependencies by default. That is why we have created a library "[taskiq-fastapi](https://pypi.org/project/taskiq-fastapi/)" to make integration with
FastAPI as smooth as possible.

Let's see what we got here. In this library, we provide you with only one public function called `init`. It takes a broker and a string path (as in uvicorn) to the fastapi application (or factory function). This function must be called in your main broker file.


```python
from taskiq import ZeroMQBroker
import taskiq_fastapi

broker = ZeroMQBroker()

taskiq_fastapi.init(broker, "my_package.application:app")

```

There are two rules to make everything work as you expect:
1. Add `TaskiqDepends` as a default value for every parameter with `Request` or `HTTPConnection` types in base dependencies.
2. Use only `TaskiqDepends` in tasks.


::: tip Cool and important note!

The Request or HTTPConnection that you'll get injected in your task is not the same request or connection you have had in your handler when you were sending the task!

:::

Many fastapi dependency functions are depend on `fastapi.Request`. We provide a mocked request to such dependencies. But taskiq cannot resolve dependencies until you explicitly specify that this parameter must be injected.

As an example. If you previously had a dependency like this:

```python
from fastapi import Request
from typing import Any

def get_redis_pool(request: Request) -> Any:
    return request.app.state.redis_pool

```

To make it resolvable in taskiq, you need to make it clear that Request object must be injected. Like this:

```python
from fastapi import Request
from taskiq import TaskiqDepends


async def get_redis_pool(request: Request = TaskiqDepends()):
    return request.app.state.redis_pool

```


Also you want to call startup of your brokers somewhere.

```python
from fastapi import FastAPI
from your_project.taskiq import broker

app = FastAPI()


@app.on_event("startup")
async def app_startup():
    if not broker.is_worker_process:
        await broker.startup()


@app.on_event("shutdown")
async def app_shutdown():
    if not broker.is_worker_process:
        await broker.shutdown()

```

And that's it. Now you can use your taskiq tasks with functions and classes that depend on FastAPI dependenices. You can find bigger examples in the [taskiq-fastapi repo](https://github.com/taskiq-python/taskiq-fastapi).
