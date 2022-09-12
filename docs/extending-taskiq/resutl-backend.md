---
order: 3
---

# Result backend

Result backends are used to store information about task execution.
To create new `result_backend` you have to implement `taskiq.abc.result_backend.AsyncResultBackend` class.


Here's a minimal example of a result backend:

@[code python](../examples/extending/result_backend.py)

::: info Cool tip!
It's a good practice to skip fetching logs from the storage unless `with_logs=True` is explicitly specified.
:::
