[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/taskiq?style=for-the-badge)](https://pypi.org/project/taskiq/)
[![PyPI](https://img.shields.io/pypi/v/taskiq?style=for-the-badge)](https://pypi.org/project/taskiq/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/taskiq?style=for-the-badge)](https://pypistats.org/packages/taskiq)

# Taskiq

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
