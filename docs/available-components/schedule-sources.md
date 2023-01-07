---
order: 4
---

# Available schedule sources

These objects are used to fetch current schedule for tasks.
Currently we have only one schedule source.

## LabelScheduleSource

This source parses labels of tasks, and if it finds a `schedule` label, it considers this task as scheduled.

The format of the schedule label is the following:

```python
@broker.task(
    schedule=[
        {
            "cron": "* * * * *", # type: str, required argument.
            "args": [], # type List[Any] | None, can be omitted.
            "kwargs": {}, # type: Dict[str, Any] | None, can be omitted.
            "labels": {}, # type: Dict[str, Any] | None, can be omitted.
        }
    ]
)
async def my_task():
    ...
```

Parameters:

- `cron` - crontab string when to run the task.
- `args` - args to use, when invoking the task.
- `kwargs` - key-word arguments to use when invoking the task.
- `labels` - additional labels to use when invoking the task.

Usage:

```python
from taskiq.scheduler import TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource

broker = ...

scheduler = TaskiqScheduler(
    broker=broker,
    sources=[LabelScheduleSource(broker)],
)
```

::: warning Cool notice!

In order to resolve all labels correctly, don't forget to import
all task modules using CLI interface.

:::
