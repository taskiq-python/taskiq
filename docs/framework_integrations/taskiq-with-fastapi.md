---
order: 1
---

# Taskiq + FastAPI

FastAPI is a highly popular async web framework in Python. It has gained its popularity because of two things:
1. It's easy to use;
2. Cool dependency injection.

In taskiq, we try to make our libraries easy to use, and We have a dependency injection too. So we have created the library "[taskiq-fastapi](https://github.com/taskiq-python/taskiq-fastapi)" to make integration with FastAPI as smooth as possible.

Let's see what we got here. In this library, we provide users with only one public function called `init`. It takes a broker and a string path (as in uvicorn) to the fastapi application (or factory function). People should call this function in their main broker file.

```python
from taskiq import ZeroMQBroker
import taskiq_fastapi

broker = ZeroMQBroker()

taskiq_fastapi.init(broker, "my_package.application:app")

```

There are two rules to make everything work as you expect:
1. Add `TaskiqDepends` as a default value for every parameter with `Request` or `HTTPConnection` types in base dependencies. Or if you use `Annotated`, please annotate these types with `TaskiqDepends`.
2. Use only `TaskiqDepends` in tasks.


::: tip Cool and important note!

The `Request` or `HTTPConnection` that you'll get injected in your task is not the same request or connection you have had in your handler when you were sending the task!

:::

Many fastapi dependency functions depend on `fastapi.Request`. We provide a mocked request to such dependencies. But taskiq cannot resolve dependencies until you explicitly specify that this parameter must be injected.

As an example. If you previously had a dependency like this:

```python
from fastapi import Request
from typing import Any

def get_redis_pool(request: Request) -> Any:
    return request.app.state.redis_pool

```

To make it resolvable in taskiq, people should mark default fastapi dependencies (such as `Request` and `HTTPConnection`) with `TaskiqDepends`. Like this:


::: tabs

@tab Annotated 3.10+

```python
from typing import Annotated
from fastapi import Request
from taskiq import TaskiqDepends


async def get_redis_pool(request: Annotated[Request, TaskiqDepends()]):
    return request.app.state.redis_pool

```

@tab default values

```python
from fastapi import Request
from taskiq import TaskiqDepends


async def get_redis_pool(request: Request = TaskiqDepends()):
    return request.app.state.redis_pool

```

:::


Also you want to call startup of your brokers somewhere.

::: tabs

@tab Lifespan (Recommended)

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from your_project.taskiq import broker


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    if not broker.is_worker_process:
        await broker.startup()
    yield
    # Shutdown
    if not broker.is_worker_process:
        await broker.shutdown()


app = FastAPI(lifespan=lifespan)
```

@tab on_event (Deprecated)

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

:::

And that's it. Now you can use your taskiq tasks with functions and classes that depend on FastAPI dependencies. You can find bigger examples in the [examples repo](https://github.com/taskiq-python/examples/).


## Testing

Testing is no different from general testing advice from articles about [testing](../guide/testing-taskiq.md). But if you use `InMemoryBroker` in your tests, you need to provide it with a custom dependency context because it doesn't run as a worker process.

Let's imagine that you have a fixture of your application. It returns a new fastapi application to use in tests.
```python

@pytest.fixture
def fastapi_app() -> FastAPI:
    return get_app()

```

Right after this fixture, we define another one.

```python
import taskiq_fastapi


@pytest.fixture(autouse=True)
def init_taskiq_deps(fastapi_app: FastAPI):
    # This is important part. Here we add dependency context,
    # this thing helps in resolving dependencies for tasks
    # for inmemory broker.
    taskiq_fastapi.populate_dependency_context(broker, fastapi_app)

    yield

    broker.custom_dependency_context = {}

```

This fixture has autouse flag, which means it would run on every test automatically.
