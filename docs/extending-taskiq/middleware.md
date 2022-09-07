---
order: 2
---

# Middlewares

Middlewares are super helpful. You can inject some code before or after task's execution.

Middlewares must implement `taskiq.abc.middleware.TaskiqMiddleware` abstract class.
Every method of a middleware can be either sync or async. Taskiq will execute it
as you expect.

For example:

::: tabs

@tab sync

@[code python](../examples/extending/middleware_sync.py)

@tab async

@[code python](../examples/extending/middleware_async.py)

:::

Also, middlewares always have reference to the current broker in `self.broker` field.
If you want to kick a message during the execution of some middleware hooks, you
may use `self.broker` to do so.

[Taskiq-pipelines](https://github.com/taskiq-python/taskiq-pipelines) uses middlewares to
call next tasks.
