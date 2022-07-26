import logging
import sys
from contextlib import AbstractContextManager
from typing import Any, List, TextIO


class LogsCollector(AbstractContextManager[TextIO]):
    """
    Context manager to collect logs.

    This useful class redirects all logs
    from stdout, stderr and root logger
    to some new target.

    It can be used like this:

    >>> logs = io.StringIO()
    >>> with LogsCollector(logs):
    >>>     print("A")
    >>>
    >>> print(f"Collected logs: {logs.get_value()}")
    """

    def __init__(self, new_target: TextIO, custom_format: str):
        self._new_target = new_target
        self._old_targets: List[TextIO] = []
        self._log_handler = logging.StreamHandler(new_target)
        self._log_handler.setFormatter(logging.Formatter(custom_format))

    def __enter__(self) -> TextIO:
        self._old_targets.append(sys.stdout)
        self._old_targets.append(sys.stderr)
        logging.root.addHandler(self._log_handler)
        sys.stdout = self._new_target
        sys.stderr = self._new_target
        return self._new_target

    def __exit__(self, *_args: Any, **_kwargs: Any) -> None:
        sys.stderr = self._old_targets.pop()
        sys.stdout = self._old_targets.pop()
        logging.root.removeHandler(self._log_handler)
