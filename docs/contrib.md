---
order: 5
---

# Contribution guide

We love contributions. This guide is for all folks who want to make taskiq better together.
We have several rules for contributors:
* Please do not add malware.
* Please make sure that your request solves the problem.

If you struggle with something or feel frustrated, you either create an issue, create a [discussions](https://github.com/orgs/taskiq-python/discussions).
page or publish a draft PR and ask your question in the description.

We have lots of tests in CI. But since CI runs from first-time contributors should be approved, you better test locally. It just takes less time to prepare PR for merging.

## Setting up environment

We use poetry for managing dependencies. To install it, please follow the official guide in [documentation](https://python-poetry.org/docs/).

After you have cloned the taskiq repo, install dependencies using this command:

```bash
poetry install
```

It will install all required dependencies.
If you want to run pytest against different python environments, please install `pyenv` using instructions from its [readme](https://github.com/pyenv/pyenv).

After pyenv is ready, you can install all python versions using this command:

```bash
pyenv install
```

## Linting

We have `pre-commit` configured with all our settings. We highly recommend you to install it as a git hook using `pre-commit install` command.

But even without installation, you can run all lints manually:

```bash
pre-commit run -a
```


## Testing

You can run `pytest` without any parameters and it will do the thing.

```bash
pytest
```

If you want to speedup testings, you can run it with `-n` option from [pytest-xdist](https://pypi.org/project/pytest-xdist/) to run tests in parallel.

```bash
pytest -n 2
```

Also we use `tox` to test against different environments. You can publish a PR to run pytest with different
python versions, but if you want to do it locally, just run `tox` command.


```bash
tox
```

Tox assumes that you've installed python versions using pyenv with command above.
