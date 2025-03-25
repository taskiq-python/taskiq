---
order: 4
---

# CLI

Core library comes with CLI program called `taskiq`, which is used to run different subcommands.

By default taskiq is shipped with only two commands: `worker` and `scheduler`. You can search for more taskiq plugins
using pypi. Some plugins may add new commands to taskiq.

## Worker

To run worker process, you have to specify the broker you want to use and modules with defined tasks.
Like this:

```bash
taskiq worker mybroker:broker_var my_project.module1 my_project.module2
```

### Auto importing

Enumerating all modules with tasks is not an option sometimes.
That's why taskiq can auto-discover tasks in current directory recursively.

We have two options for this:

- `--tasks-pattern` or `-tp`.
  It's a glob pattern of files to import. By default it is `**/tasks.py` which searches for all `tasks.py` files. May be specified multiple times.
- `--fs-discover` or `-fsd`. This option enables search of task files in current directory recursively, using the given pattern.

### Acknowledgements

The taskiq supports three types of acknowledgements:
* `when_received` - task is acknowledged when it is **received** by the worker.
* `when_executed` - task is acknowledged right after it is **executed** by the worker.
* `when_saved` - task is acknowledged when the result of execution is saved in the result backend.

This can be configured using `--ack-type` parameter. For example:

```bash
taskiq worker --ack-type when_executed mybroker:broker
```

### Type casts

One of features taskiq have is automatic type casts. For example you have a type-hinted task like this:

```python
async def task(val: int) -> int:
    return val + 1
```

If you'll call `task.kiq("2")` you'll get 3 as the returned value. Because we parse signatures of tasks and cast incoming parameters to target types.
If type-cast fails you won't throw any error. It just leave the value as is. That functionality allows you to use pydantic models, or
dataclasses as the input parameters.

To disable this pass the `--no-parse` option to the taskiq.

### Hot reload

This is annoying to restart workers every time you modify tasks. That's why taskiq supports hot-reload.
Reload is unavailable by default. To enable this feature install taskiq with `reload` extra.

::: tabs


@tab pip

```bash:no-line-numbers
pip install "taskiq[reload]"
```

@tab poetry

```bash:no-line-numbers
poetry add taskiq -E reload
```

:::

To enable this option simply pass the `--reload` or `-r` option to worker taskiq CLI.

Also this option supports `.gitignore` files. If you have such file in your directory, it won't reload worker
when you modify ignored files. To disable this functionality pass `--do-not-use-gitignore` option.

### Graceful reload (available only on Unix systems)

To perform graceful reload, send `SIGHUP` signal to the main worker process. This action will reload all workers with new code. It's useful for deployment that requires zero downtime, but without using heavy orchestration tools like Kubernetes.


```bash
taskiq worker my_module:broker
kill -HUP <main pid>
```

### Graceful and force shutdowns

If you send `SIGINT` or `SIGKILL` to the main process by pressing <kbd>Ctrl</kbd>+<kbd>C</kbd> or using the `kill` command, it will initiate the shutdown process.
By default, it will stop fetching new messages immediately after receiving the signal but will wait for the completion of all currently executing tasks.

If you don't want to wait too long for tasks to complete each time you shut down the worker, you can either send termination signals three times to the main process to perform a hard kill or configure the `--wait-tasks-timeout` to set a hard time limit for shutting down.

::: tip Cool tip
The number of signals before a hard kill can be configured with the `--hardkill-count` CLI argument.
:::


### Other parameters

* `--no-configure-logging` - disables default logging configuration for workers.
- `--log-level` is used to set a log level (default `INFO`).
* `--max-async-tasks` - maximum number of simultaneously running async tasks.
* `--max-prefetch` - number of tasks to be prefetched before execution. (Useful for systems with high message rates, but brokers should support acknowledgements).
* `--max-threadpool-threads` - number of threads for sync function execution.
* `--no-propagate-errors` - if this parameter is enabled, exceptions won't be thrown in generator dependencies.
* `--receiver` - python path to custom receiver class.
* `--receiver_arg` - custom args for receiver.
* `--ack-type` - Type of acknowledgement. This parameter is used to set when to acknowledge the task. Possible values are `when_received`, `when_executed`, `when_saved`. Default is `when_saved`.
* `max-tasks-per-child` - maximum number of tasks to be executed by a single worker process before restart.
* `--shutdown-timeout` - maximum amount of time for graceful broker's shutdown in seconds.
* `--wait-tasks-timeout` - if cannot read new messages from the broker or maximum number of tasks is reached, worker will wait for all current tasks to finish. This parameter sets the maximum amount of time to wait until shutdown.
* `--hardkill-count` - Number of termination signals to the main process before performing a hardkill.

## Scheduler

Scheduler is used to schedule tasks as described in [Scheduling tasks](./scheduling-tasks.md) section.

To run it simply run

```bash
taskiq scheduler <path to scheduler> [optional module to import]...
```

For example

```python
taskiq scheduler my_project.broker:scheduler my_project.module1 my_project.module2
```

### Parameters

Path to scheduler is the only required argument.

- `--tasks-pattern` or `-tp`.
  It's a glob pattern of files to import. By default it is `**/tasks.py` which searches for all `tasks.py` files. May be specified multiple times.
- `--fs-discover` or `-fsd`. This option enables search of task files in current directory recursively, using the given pattern.
- `--no-configure-logging` - use this parameter if your application configures custom logging.
- `--log-level` is used to set a log level (default `INFO`).
- `--skip-first-run` - skip first run of scheduler. This option skips running tasks immediately after scheduler start.
