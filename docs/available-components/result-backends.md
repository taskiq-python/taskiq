---
order: 3
---

# Available result backends

Result backends are used to store execution results.
This includes:

- return value;
- Execution time in seconds.

## Skipping result storage

Sometimes you don't need task results at all (fire-and-forget jobs, notifications, side effects only).
You can skip writing to the result backend for selected tasks with the `skip_result` label:

```python
@broker.task(skip_result=True)
async def push_notification(user_id: int) -> None:
    ...
```

Or only for a single call:

```python
await push_notification.kicker().with_labels(skip_result=True).kiq(user_id=1)
```

When `skip_result` is enabled:

- worker does **not** call `result_backend.set_result`;
- `post_save` middleware hooks are **not** executed;
- callers that use `wait_result()` will time out, because nothing is stored.

This is complementary to raising `NoResultError` from inside a task.
Use the label when the decision is static; raise `NoResultError` when you decide at runtime.

## Built-in result backends

### DummyResultBackend

This result backend doesn't do anything. It doesn't store results and cannot be used in cases,
where you need actual results.

This broker will always return `None` for any return_value. Please be careful.


## Official result backends

This result backends is not part of the core Taskiq library. But they are maintained by Taskiq developers. You can install them as a separate package.

### Redis result backend

Project link: [taskiq-redis](https://pypi.org/project/taskiq-redis/).

```bash
pip install taskiq-redis
```

### NATS result backend

Project link: [taskiq-nats](https://github.com/taskiq-python/taskiq-nats).

```bash
pip install taskiq-nats
```

## Third-party result backends

These result backends are not part of the core Taskiq library. They are maintained by other open‑source contributors. You can install them as a separate packages.

### PostgreSQL result backend

Project link: [taskiq-postgresql](https://github.com/z22092/taskiq-postgresql).

```bash
pip install taskiq-postgresql
```

### S3 result backend

Project link: [taskiq-aio-sqs](https://github.com/vonsteer/taskiq-aio-sqs).

```bash
pip install taskiq-aio-sqs
```

### YDB result backend

Project link: [taskiq-ydb](https://github.com/danfimov/taskiq-ydb).

```bash
pip install taskiq-ydb
```
