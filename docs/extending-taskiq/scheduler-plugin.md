---
order: 6
---

# Scheduler plugins

Scheduler plugins observe the scheduler's lifecycle. They are useful for
monitoring, tracing or synchronizing schedules with external systems,
like admin panels.

To create a new `scheduler plugin` you have to subclass the
[`taskiq.abc.scheduler_plugin.SchedulerPlugin`](https://github.com/taskiq-python/taskiq/blob/master/taskiq/abc/scheduler_plugin.py) class.
Every method of a plugin can be either sync or async. Taskiq will execute it
as you expect.

Plugins are observers. Unlike schedule sources they cannot cancel or modify
tasks, and exceptions raised from the `on_schedules_updated`, `pre_send` and
`post_send` hooks are logged and suppressed, so a failing plugin never breaks
scheduling.

Available hooks:

- `startup` - called during the scheduler's startup.
- `shutdown` - called during the scheduler's shutdown.
- `on_schedules_updated` - called when schedules are refreshed from a source.
  The scheduler refreshes all its sources on startup and then periodically,
  based on its update interval.
- `pre_send` - called right before a scheduled task is sent.
- `post_send` - called right after a scheduled task is sent.

For example:

@[code python](../examples/extending/scheduler_plugin.py)

Also, plugins always have a reference to the current scheduler in the
`self.scheduler` field. You can use it to access the broker or all
schedule sources, for example to kick a task or delete a schedule
during the execution of some plugin hook.

Taskiq ships one built-in plugin:
`taskiq.scheduler_plugins.TaskiqAdminSchedulerPlugin` — the scheduler
counterpart of `TaskiqAdminMiddleware`. It syncs all schedules and
registered tasks with the [taskiq-admin](https://github.com/taskiq-python/taskiq-admin)
panel and applies delete / reschedule / trigger commands created in its UI.
