import logging
from collections.abc import Sequence

from taskiq.abc.cmd import TaskiqCMD
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.run import run_worker

logger = logging.getLogger(__name__)


class WorkerCMD(TaskiqCMD):
    """Command to run workers."""

    short_help = "Helper to run workers"

    def exec(self, args: Sequence[str]) -> int | None:
        """
        Start worker process.

        Worker process creates several small
        processes in which tasks are actually processed.

        :param args: CLI arguments.
        :returns: status code.
        """
        wargs = WorkerArgs.from_cli(args)
        return run_worker(wargs)
