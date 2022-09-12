---
order: 7
---

# Scheduling tasks

Sometimes you may want to execute some tasks according to some schedule.
For example, you want to call a function every day at 2 pm.

It's easy to do with taskiq. We have primitives that can help you to solve your problems.

Let's imagine we have a module, as shown below, and we want to execute the `heavy_task` every 5 minutes.
What should we do?

@[code python](../examples/schedule/without_schedule.py)

Of course we can implement loop like this:

```python
    while True:
        await heavy_task.kiq(1)
        await asyncio.sleep(timedelta(minutes=5).total_seconds)
```

But if you have many schedules it may be a little painful to implement. So let me introuce you the `TaskiqScheduler`.
Let's add scheduler to our module.

@[code python](../examples/schedule/intro.py)

That's it.

Now we need to start our scheduler with the `taskiq scheduler` command. Like this:

```bash:no-line-numbers
taskiq scheduler module:scheduler
```

::: danger Be careful!

Please always run only one instance of the scheduler!
If you run more than one scheduler at a time, please be careful since
it may execute one task N times, where N is the number of running scheduler instances.

:::

This command will import the scheduler you defined and start sending tasks to your broker.

You can check list of available schedule sources in the [Available schedule sources](../available-components/schedule-sources.md) section.


## Multiple sources

Sometimes you may want to use multiple sources to assemble a schedule for tasks. The `TaskiqScheduler` can do so.
But it's obvious how to merge schedules from different sources.

That's why you can pass a custom merge function to resolve all possible conflicts or if you want to have more
complex logic aside from sources. For example, filter out some task schedules.

Currently we have only two default functions to merge tasks. You can find them in the `taskiq.scheduler.merge_functions` module.

* `preserve_all` - simply adds new schedules to the old ones.
* `only_unique` - adds scheudle only if it was not added by previous sources.

Every time we update schedule it gets task from the source and executes this function to merge them together.
