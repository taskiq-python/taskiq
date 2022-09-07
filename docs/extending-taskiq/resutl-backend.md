---
order: 3
---

# Result backend

Result backends are used to store information about task execution.
To create new `result_backend` you have to implement `taskiq.abc.result_backend.AsyncResultBackend` class.


Here's a minimal example of a result backend:

```python
from typing import TypeVar
from taskiq import TaskiqResult
from taskiq.abc.result_backend import AsyncResultBackend

_ReturnType = TypeVar("_ReturnType")


class MyResultBackend(AsyncResultBackend[_ReturnType]):
    async def startup(self) -> None:
        """Do something when starting broker."""

    async def shutdown(self) -> None:
        """Do something on shutdown."""

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        # Here you must set result somewhere.
        pass

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        # Here you must retrieve result by id.

        # Logs is a part of a result.
        # Here we have a parameter whether you want to
        # fetch result with logs or not, because logs
        # can have a lot of info and sometimes it's critical
        # to get only needed information.
        pass

    async def is_result_ready(
        self,
        task_id: str,
    ) -> bool:
        # This function checks if result of a task exists,
        # without actual fetching the result.
        pass

```

::: info Cool tip!
It's a good practice to skip fetching logs from the storage unless `with_logs=True` is explicitly specified.
:::
