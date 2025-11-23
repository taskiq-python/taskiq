import logging
import sys
from collections.abc import Generator
from contextlib import contextmanager
from typing import IO, Any, TextIO


class Redirector:
    """A class to write to multiple streams."""

    def __init__(self, *streams: IO[Any]) -> None:
        self.streams = streams

    def write(self, message: Any) -> None:
        """
        This write request writes to all available streams.

        :param message: message to write.
        """
        for stream in self.streams:
            stream.write(message)


@contextmanager
def log_collector(
    new_target: TextIO,
    custom_format: str,
) -> "Generator[TextIO, None, None]":
    """
    Context manager to collect logs.

    This useful class redirects all logs
    from stdout, stderr and root logger
    to some new target.

    It can be used like this:

    >>> logs = io.StringIO()
    >>> with log_collector(logs, "%(levelname)s %(message)s"):
    >>>     print("A")
    >>>
    >>> print(f"Collected logs: {logs.get_value()}")

    :param new_target: new target for logs. All
        logs are written in new_target.
    :param custom_format: custom format for
        collected logging calls.
    :yields: new target.
    """
    old_targets: list[TextIO] = []
    log_handler = logging.StreamHandler(new_target)
    log_handler.setFormatter(logging.Formatter(custom_format))

    old_targets.extend([sys.stdout, sys.stderr])
    logging.root.addHandler(log_handler)
    sys.stdout = Redirector(new_target, sys.stdout)  # type: ignore
    sys.stderr = Redirector(new_target, sys.stderr)  # type: ignore

    try:
        yield new_target
    finally:
        sys.stderr = old_targets.pop()
        sys.stdout = old_targets.pop()
        logging.root.removeHandler(log_handler)
