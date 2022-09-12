import logging
from typing import Sequence

from taskiq.abc.cmd import TaskiqCMD
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.run import run_worker

logger = logging.getLogger(__name__)


class WorkerCMD(TaskiqCMD):
    """Command to run workers."""

    short_help = "Helper to run workers"

    def exec(self, args: Sequence[str]) -> None:
        """
        Start worker process.

        Worker process creates several small
        processes in which tasks are actually processed.

        :param args: CLI arguments.
        """
        wargs = WorkerArgs.from_cli(args)
        run_worker(wargs)
