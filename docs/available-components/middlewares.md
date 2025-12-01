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
from taskiq import ZeroMQBroker, SimpleRetryMiddleware

broker = ZeroMQBroker().with_middlewares(
    SimpleRetryMiddleware(default_retry_count=3),
)
```

After that you can add a label to task that you want to restart on error.

```python

@broker.task(retry_on_error=True, max_retries=20)
async def test():
    raise Exception("AAAAA!")

```

`retry_on_error` enables retries for a task. `max_retries` is the maximum number of times,.

## Smart Retry Middleware

The `SmartRetryMiddleware` automatically retries tasks with flexible delay settings and retry strategies when errors occur. This is particularly useful when tasks fail due to temporary issues, such as network errors or temporary unavailability of external services.

### Key Features:

* **Retry Limits**: Set the maximum number of retry attempts (`max_retries`).
* **Delay Before Retry**: Define a fixed delay or use additional strategies.
* **Jitter**: Adds random delay variations to distribute retries evenly and prevent simultaneous task execution.
* **Exponential Backoff**: Increases the delay for each subsequent retry, useful for gradually recovering system performance.
* **Custom Schedule Source (`schedule_source`)**: Enables using a custom schedule source to manage delayed task execution.

### Middleware Integration

To use `SmartRetryMiddleware`, add it to the list of middlewares in your broker:

```python
from taskiq import ZeroMQBroker
from taskiq.middlewares import SmartRetryMiddleware

broker = ZeroMQBroker().with_middlewares(
    SmartRetryMiddleware(
        default_retry_count=5,
        default_delay=10,
        use_jitter=True,
        use_delay_exponent=True,
        max_delay_exponent=120
    ),
)
```

### Using Middleware with Tasks

To enable retries for a specific task, specify the following parameters:

```python
@broker.task(retry_on_error=True, max_retries=10, delay=15)
async def my_task():
    raise Exception("Error, retrying!")
```

* `retry_on_error`: Enables the retry mechanism for the specific task.
* `max_retries`: Maximum number of retries (overrides middleware default).
* `delay`: Initial delay before retrying the task, in seconds.

### Usage Recommendations

Use jitter and exponential backoff to avoid repetitive load peaks, especially in high-load systems. Choose appropriate `max_delay_exponent` values to prevent excessively long intervals between retries if your task execution is time-sensitive.



### Prometheus middleware

You can enable prometheus metrics for workers by adding `PrometheusMiddleware`.
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
from taskiq import ZeroMQBroker, PrometheusMiddleware

broker = ZeroMQBroker().with_middlewares(
    PrometheusMiddleware(server_addr="0.0.0.0", server_port=9000),
)
```

After that, metrics will be available at port 9000. Of course, this parameter can be configured.
If you have other metrics, they'll be shown as well.

### OpenTelemetry Middleware

You can enable opentelemetry tracing for workers by adding `OpenTelemetryMiddleware` or using `TaskiqInstrumentor` (preferred).

```bash
pip install "taskiq[opentelemetry]"
```

::: tabs


@tab instrumentor

```python
from taskiq import ZeroMQBroker
from taskiq.instrumentation import TaskiqInstrumentor

TaskiqInstrumentor().instrument()
broker = ZeroMQBroker()
```

@tab middleware

```python
from taskiq import ZeroMQBroker
from taskiq.middlewares.opentelemetry_middleware import OpenTelemetryMiddleware

broker = ZeroMQBroker().with_middlewares(
   OpenTelemetryMiddleware,
)
```

:::

Auto-instrumentation is also supported.
