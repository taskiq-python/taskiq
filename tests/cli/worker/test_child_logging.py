import logging
from unittest.mock import patch

import pytest

from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.run import configure_child_logging


@pytest.mark.parametrize("start_method", ["spawn", "forkserver"])
def test_logging_is_configured_in_fresh_interpreters(start_method: str) -> None:
    """Spawn and forkserver children don't inherit the parent's logging config."""
    args = WorkerArgs.from_cli(
        ["example:broker", "--log-level", "WARNING", "--log-format", "%(message)s"],
    )

    with (
        patch("taskiq.cli.worker.run.get_start_method", return_value=start_method),
        patch("taskiq.cli.worker.run.logging.basicConfig") as basic_config,
    ):
        configure_child_logging(args)

    basic_config.assert_called_once_with(level=logging.WARNING, format="%(message)s")


def test_logging_is_inherited_with_fork() -> None:
    """Forked children already have the parent's logging configuration."""
    args = WorkerArgs.from_cli(["example:broker"])

    with (
        patch("taskiq.cli.worker.run.get_start_method", return_value="fork"),
        patch("taskiq.cli.worker.run.logging.basicConfig") as basic_config,
    ):
        configure_child_logging(args)

    basic_config.assert_not_called()


@pytest.mark.parametrize("start_method", ["spawn", "forkserver"])
def test_no_configure_logging_is_respected(start_method: str) -> None:
    args = WorkerArgs.from_cli(["example:broker", "--no-configure-logging"])

    with (
        patch("taskiq.cli.worker.run.get_start_method", return_value=start_method),
        patch("taskiq.cli.worker.run.logging.basicConfig") as basic_config,
    ):
        configure_child_logging(args)

    basic_config.assert_not_called()
