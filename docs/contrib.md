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

We use uv for managing dependencies. To install it, please follow the official guide in [documentation](https://docs.astral.sh/uv/getting-started/installation/).

After you have cloned the taskiq repo, install dependencies using this command:

```bash
uv sync --all-extras
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

## Working with documentation

For documentation we use [VuePress 2](https://vuepress.vuejs.org/). To run documentation locally, use steps below.

First of all, install dependencies for documentation. We recommend to use `pnpm` for managing dependencies, but `package.json` is compatible with `npm` and `bun` for example as well:

```bash
pnpm i
```

After that, you can run documentation server with hot-reloading using:

```bash
pnpm docs:dev
```

If you want to check how documentation looks like in production mode, you can build it and then serve using:

```bash
pnpm docs:build
pnpm docs:serve
```
