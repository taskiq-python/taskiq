[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/taskiq?style=for-the-badge)](https://pypi.org/project/taskiq/)
[![PyPI](https://img.shields.io/pypi/v/taskiq?style=for-the-badge)](https://pypi.org/project/taskiq/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/taskiq?style=for-the-badge)](https://pypistats.org/packages/taskiq)

<div align="center">
<a href="https://taskiq-python.github.io/"><img src="https://raw.githubusercontent.com/taskiq-python/taskiq/master/imgs/logo.svg" width=600></a>
<hr/>
</div>

Taskiq is an asynchronous distributed task queue for python.
This project takes inspiration from big projects such as [Celery](https://docs.celeryq.dev) and [Dramatiq](https://dramatiq.io/).
But taskiq can send and run both the sync and async functions.
Also, we use [PEP-612](https://peps.python.org/pep-0612/) to provide the best autosuggestions possible. But since it's a new PEP, I encourage you to use taskiq with VS code because Pylance understands all types correctly.

# Installation

This project can be installed using pip:
```bash
pip install taskiq
```

Or it can be installed directly from git:

```bash
pip install git+https://github.com/taskiq-python/taskiq
```

You can read more about how to use it in our docs: https://taskiq-python.github.io/.


# Local development


## Linting

We use pre-commit to do linting locally.

After cloning this project, please install [pre-commit](https://pre-commit.com/#install). It helps fix files before committing changes.

```bash
pre-commit install
```


## Testing

Pytest can run without any additional actions or options.

```bash
pytest
```

## Docs

To run docs locally, you need to install [yarn](https://yarnpkg.com/getting-started/install).

First, you need to install dependencies.
```
yarn install
```

After that you can set up a docs server by running:

```
yarn docs:dev
```
