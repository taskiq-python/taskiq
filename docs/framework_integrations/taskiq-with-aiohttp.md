---
order: 2
---

# Taskiq + AioHTTP

AioHTTP is a framework for building robust applications. We created several libraries to make the experience with AioHTTP even better.

# Dependency injection for AioHTTP

We created a library [aiohttp-deps](https://pypi.org/project/aiohttp-deps/) to add FastAPI-like dependency injection in AioHTTP.

To install it, simply run:

```python
pip install "aiohttp-deps"
```

After the installation, please add startup event to your application to initialize dependencies context.

```python
from aiohttp import web
import aiohttp_deps


app = web.Application()

# This startup event makes all the magic happen.
# It parses current handlers and create dependency graphs for them.
app.on_startup.append(aiohttp_deps.init)

web.run_app(app)
```

You can read more about dependency injection and available dependencies in the project's [README.md](https://github.com/taskiq-python/aiohttp-deps).



## Adding taskiq integration

We highly recommend using aiohttp with aiohttp-deps because it allows us to reuse the same dependencies for your handlers and tasks. First of all, you should install the [taskiq-aiohttp](https://pypi.org/project/taskiq-aiohttp/) library.

```python
pip install "taskiq-aiohttp"
```

After the installation is complete, add an initialization function call to your broker's main file so it becomes something like this:

```python
import taskiq_aiohttp

broker = MyBroker()

# The second argument is a path to web.Application variable.
# Also you can provide here a factory function that takes no
# arguments and returns an application. This function can be async.
taskiq_aiohttp.init(broker, "my_project.main:app")
```

From this point, you'll be able to reuse the same dependencies as with `aiohttp-deps`.
Let's take a look at this function:

::: tabs

@tab Annotated 3.10+

```python
from aiohttp import web
from typing import Annotated
from taskiq import TaskiqDepends
from my_project.tkq import broker

@broker.task
async def my_task(app: Annotated[web.Application, TaskiqDepends()]):
    ...

```

@tab default values

```python
from aiohttp import web
from taskiq import TaskiqDepends
from my_project.tkq import broker

@broker.task
async def my_task(app: web.Application = TaskiqDepends()):
    ...

```

:::

In this example, we depend on the current application. We can use its state in a current task or any other dependency. We can take db_pool from your application's state, which is the same pool, as the one you've created on AiohTTP's startup.
But this application is only a mock of your application. It has correct types and all your variables that you filled on startup, but it doesn't handle any request.
This integration adds two main dependencies:
* web.Application - current application.
* web.Request - mocked request. This request only exists to be able to use the same dependencies.

You can find more detailed examples in the [examples repo](https://github.com/taskiq-python/examples).

## Testing

Writing tests for AioHTTP with taskiq is as easy as writing tests for the aiohttp application. The only difference is that, if you want to use InMemoryBroker, then you need to add context for dependency injection. It's easier to call `populate_context` when creating a `test_client` fixture.

```python
import taskiq_aiohttp

@pytest.fixture
async def test_client(
    app: web.Application,
) -> AsyncGenerator[TestClient, None]:
    """
    Create a test client.

    This function creates a TestServer
    and a test client for the application.

    Also this fixture populates context
    with needed variables.

    :param app: current application.
    :yield: ready to use client.
    """
    loop = asyncio.get_running_loop()
    server = TestServer(app)
    client = TestClient(server, loop=loop)

    await client.start_server()

    # This is important part.
    # Since InMemoryBroker doesn't
    # run as a worker process, we have to populate
    # broker's context by hand.
    taskiq_aiohttp.populate_context(
        broker=broker,
        server=server.runner.server,
        app=app,
        loop=loop,
    )

    yield client

    broker.custom_dependency_context = {}
    await client.close()
```
