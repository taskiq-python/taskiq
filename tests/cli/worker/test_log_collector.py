import logging
import sys
from io import StringIO

from taskiq.cli.worker.log_collector import log_collector


def test_log_collector_std_success() -> None:
    """Tests that stdout and stderr calls are collected correctly."""
    log = StringIO()
    with log_collector(log, "%(message)s"):
        print("log1")
        print("log2", file=sys.stderr)
    assert log.getvalue() == "log1\nlog2\n"


def test_log_collector_logging_success() -> None:
    """Tests that logging calls are collected correctly."""
    log = StringIO()
    with log_collector(log, "%(levelname)s %(message)s"):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        logger.info("log1")
        logger.warning("log2")
        logger.debug("log3")
    assert log.getvalue() == "INFO log1\nWARNING log2\nDEBUG log3\n"
