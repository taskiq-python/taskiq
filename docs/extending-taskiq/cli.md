---
order: 4
---

# CLI

You can easily add new subcommands to taskiq. All default subcommands also use this mechanism,
since it's easy to use.

At first you need to add a class that implements `taskiq.abc.cmd.TaskiqCMD` abstract class.

@[code python](../examples/extending/cli.py)

In the `exec` method, you should parse incoming arguments. But since all CLI arguments to taskiq are shifted you can ignore the `args` parameter.

Also, you can use your favorite tool to build CLI, like [click](https://click.palletsprojects.com/) or [typer](https://typer.tiangolo.com/).

After you have such class, you need to add entrypoint that points to that class.

::: tabs

@tab setuptools setup.py

```python
from setuptools import setup

setup(
    # ...,
    entry_points={
        'taskiq_cli': [
            'demo = my_project.cmd:MyCommand',
        ]
    }
)
```

@tab setuptools pyproject.toml

```toml
[project.entry-points.taskiq_cli]
demo = "my_project.cmd:MyCommand"
```

@tab poetry

```toml
[tool.poetry.plugins.taskiq_cli]
demo = "my_project.cmd:MyCommand"
```

:::

You can read more about entry points in [python documentation](https://packaging.python.org/en/latest/specifications/entry-points/).
The subcommand name is the same as the name of the entry point you've created.

```bash
$ taskiq demo --help
usage: demo [-h] [--test TEST]

optional arguments:
  -h, --help   show this help message and exit
  --test TEST  My test parameter.
```

```bash
$ taskiq demo --test aaa
Namespace(test='aaa')
```
