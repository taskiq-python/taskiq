---
order: 9
---

# Testing with taskiq

Every time we write programs, we want them to be correct. To achieve this, we use tests.
Taskiq allows you to write tests easily as if tasks were normal functions.

Let's dive into examples.

## Preparations

### Environment setup
For testing you maybe don't want to use actual distributed broker. But still you want to validate your logic.
Since python is an interpreted language, you can easily replace you broker with another one if the expression is correct.

We can set an environment variable, that indicates that currently we're running in testing environment.

::: tabs

@tab linux|macos


```bash
export ENVIRONMENT="pytest"
pytest -vv
```

@tab windows

```powershell
$env:ENVIRONMENT = 'pytest'
pytest -vv
```

:::


Or we can even tell pytest to set this environment for us, just before executing tests using [pytest-env](https://pypi.org/project/pytest-env/) plugin.

::: tabs

@tab pytest.ini

```ini
[pytest]
env =
    ENVIRONMENT=pytest
```

@tab pyproject.toml

```toml
[tool.pytest.ini_options]
env = [
    "ENVIRONMENT=pytest",
]
```

:::

### Async tests

Since taskiq is fully async, we suggest using [anyio](https://anyio.readthedocs.io/en/stable/testing.html) to run async functions in pytest. Install the [lib](https://pypi.org/project/anyio/) and place this fixture somewhere in your root `conftest.py` file.

```python
@pytest.fixture
def anyio_backend():
    return 'asyncio'
```

After the preparations are done, we need to modify the broker's file in your project.

@[code python](../examples/testing/main_file.py)

As you can see, we added an `if` statement. If the expression is true, we replace our broker with an imemory broker.
The main point here is to not have an actual connection during testing. It's useful because inmemory broker has
the same interface as a real broker, but it doesn't send tasks actually.

## Testing tasks

Let's define a task.

```python
from your_project.taskiq import broker

@broker.task
async def parse_int(val: str) -> int:
    return int(val)
```

This simple task may be defined anywhere in your project. If you want to test it,
just import it and call as a normal function.

```python
import pytest
from your_project.tasks import parse_int

@pytest.mark.anyio
async def test_task():
    assert await parse_int("11") == 11
```

And that's it. Test should pass.

What if you want to test a function that uses task. Let's define such function.

```python
from your_project.taskiq import broker

@broker.task
async def parse_int(val: str) -> int:
    return int(val)


async def parse_and_add_one(val: str) -> int:
    task = await parse_int.kiq(val)
    result = await task.wait_result()
    return result.return_value + 1
```

And since we replaced our broker with `InMemoryBroker`, we can just call it.
It would work as you expect and tests should pass.

```python
@pytest.mark.anyio
async def test_add_one():
    assert await parse_and_add_one("11") == 12
```

## Dependency injection

If you use dependencies in your tasks, you may think that this can become a problem. But it's not.
Here's what we came up with. We added a method called `add_dependency_context` to the broker.
It sets base dependencies for dependency resolution. You can use it for tests.

Let's add a task that depends on `Path`. I guess this example is not meant to be used in production code bases, but it's suitable for illustration purposes.

```python
from pathlib import Path
from taskiq import TaskiqDepends

from your_project.taskiq import broker


@broker.task
async def modify_path(some_path: Path = TaskiqDepends()):
    return some_path.parent / "taskiq.py"

```

To test the task itself, it's not different to the example without dependencies, but we jsut need to pass all
expected dependencies manually as function's arguments or key-word arguments.

```python
import pytest
from your_project.taskiq import broker

from pathlib import Path

@pytest.mark.anyio
async def test_modify_path():
    modified = await modify_path(Path.cwd())
    assert str(modified).endswith("taskiq.py")

```

But what if we want to test task execution? Well, you don't need to provide dependencies manually, you
must mutate dependency_context before calling a task. We suggest to do it in fixtures.

```python
import pytest
from your_project.taskiq import broker
from pathlib import Path


# We use autouse, so this fixture
# is called automatically before all tests.
@pytest.fixture(scope="function", autouse=True)
async def init_taskiq_dependencies():
    # Here we use Path, but you can use other
    # pytest fixtures here. E.G. FastAPI app.
    broker.add_dependency_context({Path: Path.cwd()})

    yield

    # After the test we clear all custom dependencies.
    broker.custom_dependency_context = {}

```

This fixture will update dependency context for our broker before
every test. Now tasks with dependencies can be used. Let's try it out.

```python
@pytest.mark.anyio
async def test_modify_path():
    task = await modify_path.kiq()
    result = await task.wait_result()
    assert str(result.return_value).endswith("taskiq.py")

```

This should pass. And that's it for now.
