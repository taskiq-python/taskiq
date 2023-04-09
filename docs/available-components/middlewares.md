---
order: 5
---

# Available middlewares

Middlewares allow you to execute code when specific event occurs.
Taskiq has several default middlewares.

### Simple retry middleware

This middleware allows you to restart functions on errors. If exception was raised during task execution,
the task would be resent with same parameters.

To enable this middleware, add it to the list of middlewares for a broker.

```python
from taskiq import SimpleRetryMiddleware

broker = ...

broker.add_middlewares(SimpleRetryMiddleware(default_retry_count=3))
```

After that you can add a label to task that you want to restart on error.

```python

@broker.task(retry_on_error=True, max_retries=20)
async def test():
    raise Exception("AAAAA!")

```

`retry_on_error` enables retries for a task. `max_retries` is the maximum number of times,.

### Prometheus middleware

You can enable prometheus metrics for workers by adding PrometheusMiddleware.
To do so, you need to install `prometheus_client` package or you can install metrics extras for taskiq.

::: tabs


@tab only prometheus

```bash
pip install "prometheus_client"
```

@tab taskiq with extras

```bash
pip install "taskiq[metrics]"
```

:::


```python
from taskiq import PrometheusMiddleware

broker = ...

broker.add_middlewares(PrometheusMiddleware(server_addr="0.0.0.0", server_port=9000))
```

After that, metrics will be available at port 9000. Of course, this parameter can be configured.
If you have other metrics, they'll be shown as well.
