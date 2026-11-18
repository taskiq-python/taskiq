---
title: Error handling
order: 10
---

# Error handling

When a task raises an exception, the worker doesn't crash. It catches the exception, logs it and stores it in the 
`TaskiqResult` along with everything else. What you do with it afterwards depends on where you want to handle the error:
on the client side, where the result is awaited, or on the worker side, where the exception happened.

## Handling errors on the client

`TaskiqResult` has two fields related to errors:

* `is_err` - `True` if the task has failed;
* `error` - the exception itself, or `None` if the task succeeded.

@[code python](../examples/errors/result_error.py)

If you'd rather work with exceptions than with flags, use the `raise_for_error` method. It raises the stored exception 
if there is one and returns the result itself otherwise, so it can be chained right after `wait_result`.

@[code python](../examples/errors/raise_for_error.py)

Tracebacks are not transferred to the client. Only the exception type, its arguments and the chain of `__cause__`/`__context__` 
exceptions are serialized. Sending tracebacks over the network is unreliable and may leak sensitive information from the
worker, so Taskiq intentionally doesn't do it.

Also, the client has to be able to import the exception class to restore it. If the exception is defined in a module 
unavailable on the client side, Taskiq creates a synthetic exception class with the same name in the `taskiq.exceptions`
namespace, so `except MyError` won't catch it, while `except Exception` still will.

## Handling errors on the worker

Client-side handling is only possible if you use a result backend and wait for the result. Fire-and-forget tasks have 
nobody to report to. Besides, sometimes you want to react to every failure in one place instead of duplicating 
the same handling in every caller.

For these cases, use the `on_error` hook of a middleware. It's called on the worker right after the exception is caught, so the full traceback is still available.

@[code python](../examples/errors/error_middleware.py)

That's the recommended way to integrate taskiq with error tracking systems. For example, here's how you can send all task failures to Sentry:

```python
from typing import Any

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration, ignore_logger

from taskiq import (
    TaskiqEvents,
    TaskiqMessage,
    TaskiqMiddleware,
    TaskiqResult,
    TaskiqState,
)


class SentryMiddleware(TaskiqMiddleware):
    def on_error(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
        exception: BaseException,
    ) -> None:
        sentry_sdk.capture_exception(
            exception,
            tags={
                "taskiq_task_name": message.task_name,
                "taskiq_task_id": message.task_id,
            },
        )


# Here `broker` is the broker you defined for your project.
broker = broker.with_middlewares(SentryMiddleware())


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
def init_sentry(_: TaskiqState) -> None:
    # We ignore this logger, because its events may overwrite the exception 
    # we capture in the middleware with new tags.
    ignore_logger("taskiq.receiver.receiver")
    sentry_sdk.init(
        "https://key@your-sentry-instance",
        environment="dev",
        integrations=[LoggingIntegration()],
    )
```

If your framework integration initializes Sentry on its own, you can drop the startup event and keep the middleware only.

::: tip Retries

If you want failed tasks to be retried instead of being reported, take a look at the [`SimpleRetryMiddleware`](../available-components/middlewares.md)
or the [`SmartRetryMiddleware`](../available-components/middlewares.md).

:::

## Errors in dependencies

Generator dependencies can catch exceptions raised by the task they are injected into. It's useful if you want to roll 
back a transaction that was opened in the dependency. This is described in 
[State and Dependencies](./state-and-deps.md#exception-handling), along with the `--no-propagate-errors` option 
that turns this behaviour off.

## Skipping the result

Sometimes a task shouldn't store any result at all. Raising `NoResultError` inside a task tells the worker to skip 
saving the result to the result backend, so waiting for the result of such a task will time out. 
Note that `NoResultError` is still treated as an exception internally, so `on_error` middlewares are called for it as well.

```python
from taskiq import NoResultError


@broker.task
async def my_task() -> None:
    raise NoResultError
```
