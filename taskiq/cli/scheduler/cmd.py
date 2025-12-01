import asyncio
from collections.abc import Sequence

from taskiq.abc.cmd import TaskiqCMD
from taskiq.cli.scheduler.args import SchedulerArgs
from taskiq.cli.scheduler.run import run_scheduler


class SchedulerCMD(TaskiqCMD):
    """Command for taskiq scheduler."""

    short_help = "Run task scheduler"

    def exec(self, args: Sequence[str]) -> None:
        """
        Run task scheduler.

        This function starts scheduler function.

        It periodically loads schedule for tasks
        and executes them.

        :param args: CLI arguments.
        """
        parsed = SchedulerArgs.from_cli(args)
        asyncio.run(run_scheduler(parsed))
