---
order: 4
---

# Available schedule sources

These objects are used to fetch current schedule for tasks.
Currently we have only one schedule source.

## RedisScheduleSource

This source is capable of adding new schedules in runtime. It uses Redis as a storage for schedules.
To use this source you need to install `taskiq-redis` package.

```python
from taskiq_redis import RedisScheduleSource

from taskiq import TaskiqScheduler

redis_source = RedisScheduleSource("redis://localhost:6379/0")
scheduler = TaskiqScheduler(broker, sources=[redis_source])
```

For more information on how to use dynamic schedule sources read [Dynamic scheduling section](../guide/scheduling-tasks.md#dynamic-scheduling).


## LabelScheduleSource

This source parses labels of tasks, and if it finds a `schedule` label, it considers this task as scheduled.

The format of the schedule label is the following:

```python
@broker.task(
    schedule=[
        {
            "cron": "* * * * *", # type: str, either cron or time shoule be specified.
            "cron_offset": None # type: str | timedelta | None, can be ommited.
            "time": None  # type: datetime | None, either cron or time shoule be specified.
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
- `cron_offset` - timezone offset for cron values. Explained [here](../guide/scheduling-tasks.md#working-with-timezones)
- `time` - specific time when send the task.
- `args` - args to use, when invoking the task.
- `kwargs` - key-word arguments to use when invoking the task.
- `labels` - additional labels to use when invoking the task.

To enable this source, just add it to the list of sources:

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
