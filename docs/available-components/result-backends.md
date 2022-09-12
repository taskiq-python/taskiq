---
order: 3
---

# Available result backends

Result backends are used to store execution results.
This includes:
* Captured logs;
* return value;
* Execution time in seconds.


## DummyResultBackend

This result backend doesn't do anything. It doesn't store results and cannot be used in cases,
where you need actual results.

This broker will always return `None` for any return_value. Please be careful.


## Redis result backend

This result backend is not part of the core taskiq library. You can install it as a separate package [taskiq-redis](https://pypi.org/project/taskiq-redis/).

```bash
pip install taskiq-redis
```

You can read more about parameters and abilities of this broker in [README.md](https://github.com/taskiq-python/taskiq-redis).
