---
order: 8
---

# Scheduling tasks

Sometimes you may want to execute some tasks according to some schedule.
For example, you maybe want to call a function every day at 2 pm.

That's not a problem if you use taskiq. We have primitives that can help you to solve your problems.

Let's imagine we have a module, as shown below, and we want to execute the `heavy_task` every 5 minutes.
What should we do?

@[code python](../examples/schedule/without_schedule.py)

Of course we can implement loop like this:

```python
    while True:
        await heavy_task.kiq(1)
        await asyncio.sleep(timedelta(minutes=5).total_seconds)
```

But if you have many schedules it may be a little painful to implement. So let me introduce you the `TaskiqScheduler`.
Let's add scheduler to our module.

@[code python](../examples/schedule/intro.py)

That's it.

Now we need to start our scheduler with the `taskiq scheduler` command. Like this:

```bash:no-line-numbers
taskiq scheduler module:scheduler
```

::: caution Be careful!

Please always run only one instance of the scheduler!
If you run more than one scheduler at a time, please be careful since
it may execute one task N times, where N is the number of running scheduler instances.

:::

This command will import the scheduler you defined and start sending tasks to your broker.

::: tip Cool tip!

The scheduler doesn't execute tasks. It only sends them.

:::

You can check list of available schedule sources in the [Available schedule sources](../available-components/schedule-sources.md) section.

## Multiple sources

Sometimes you may want to use multiple sources to assemble a schedule for tasks. The `TaskiqScheduler` can do so.
But it's obvious how to merge schedules from different sources.

That's why you can pass a custom merge function to resolve all possible conflicts or if you want to have more
complex logic aside from sources. For example, filter out some task schedules.

Currently we have only two default functions to merge tasks. You can find them in the `taskiq.scheduler.merge_functions` module.

- `preserve_all` - simply adds new schedules to the old ones.
- `only_unique` - adds schedule only if it was not added by previous sources.

Every time we update schedule it gets task from the source and executes this function to merge them together.


## Working with timezones

Sometimes, you want to be specific in terms of time zones. We have you covered.
Our `ScheduledTask` model has fields for that. Use these fields or not, it's up to the specific schedule source.

Taskiq scheduler assumes that if time has no specific timezone, it's in [UTC](https://www.wikiwand.com/en/Coordinated_Universal_Time). Sometimes, this behavior might not be convinient for developers.

For the `time` field of `ScheduledTask` we use timezone information from datetime to check if a task should run.

For `cron` tasks, we have an additional field called `cron_offset` that can be used to specify
an offset of the cron task. An offset can be a string like `Europe/Berlin` or an instance of the `timedelta` class.

## Skipping first run

By default, when you start the scheduler it will get all tasks from the schedule source and check whether they should have been executed in this minute. If tasks should have been executed, they will be executed.

This behaviour might be not convinient for some developers. For example, if you have a task that should be executed on every minute, it will be executed once you start the scheduler, even if it was executed a few seconds ago.

To avoid this behaviour, you can pass the `--skip-first-run` flag to the `taskiq scheduler` command. In this case, the scheduler will wait until the start of the next minute and then start executing tasks.

```bash:no-line-numbers
taskiq scheduler module:scheduler --skip-first-run
```
