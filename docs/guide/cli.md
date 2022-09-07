---
order: 4
---

# CLI

Core library comes with CLI programm called `taskiq`, which is used to run workers.

To run it you have to specify the broker you want to use and modules with defined tasks.
Like this:

```bash
taskiq mybroker:broker_var my_project.module1 my_project.module2
```

## Autoimporting

Enumerating all modules with tasks is not an option sometimes.
That's why taskiq can autodiscover tasks in current directory recursively.

We have two options for this:
* `--tasks-pattern` or `-tp`.
    It's a name of files to import. By default is searches for all `tasks.py` files.
* `--fs-discover` or `-fsd`. This option enables search of task files in current directory recursively, using the given pattern.


## Type casts

One of features taskiq have is automatic type casts. For examle you have a type-hinted task like this:
```python
async def task(val: int) -> int:
    return val + 1
```

If you'll call `task.kiq("2")` you'll get 3 as the returned value. Because we parse signatures of tasks and cast incoming parameters to target types.
If type-cast fails you won't throw any error. It just leave the value as is. That functionality allows you to use pydantic models, or
dataclasses as the input parameters.

To disable this pass the `--no-parse` option to the taskiq.

## Hot reload

This is annoying to restart workers every time you modify tasks. That's why taskiq supports hot-reload.
To enable this option simply pass the `--reload` or `-r` option to taskiq CLI.

Also this option supports `.gitignore` files. If you have such files in your directory. It won't reload worker,
if you cange ignored file's contents. To disable this functionality pass `--do-not-use-gitignore` option.
