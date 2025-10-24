---
order: 5
---

# Schedule source

Schedule sources are used to get schedule for tasks.
To create new `schedule source` you have to implement the `taskiq.abc.schedule_source.ScheduleSource` abstract class.

Here's a minimal example of a schedule source:

@[code python](../examples/extending/schedule_source.py)

You can implement a schedule source that write schedules in the database and have delayed tasks in runtime.

::: info Cool tip!
You can also use `LabelScheduleSource` as a base class for your schedule source
if you want to parse schedules from task labels and don't want to implement logic for this from scratch.
:::
