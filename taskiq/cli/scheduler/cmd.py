import asyncio
from collections.abc import Sequence
from functools import partial

import anyio

from taskiq.abc.cmd import TaskiqCMD
from taskiq.cli.scheduler.args import SchedulerArgs
from taskiq.cli.scheduler.run import run_scheduler
from taskiq.cli.utils import create_event_loop, resolve_loop_factory


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
        if parsed.loop_factory is None:
            asyncio.run(run_scheduler(parsed))
            return
        loop_factory = resolve_loop_factory(
            parsed.loop_factory,
            app_dir=parsed.app_dir,
        )
        anyio.run(
            run_scheduler,
            parsed,
            backend_options={
                "loop_factory": partial(create_event_loop, loop_factory),
            },
        )
