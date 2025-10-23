---
order: 3
---

# Available result backends

Result backends are used to store execution results.
This includes:

- return value;
- Execution time in seconds.

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
