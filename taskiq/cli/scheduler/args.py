from argparse import ZERO_OR_MORE, ArgumentDefaultsHelpFormatter, ArgumentParser
from dataclasses import dataclass
from typing import List, Optional, Sequence, Union

from taskiq.cli.common_args import LogLevel
from taskiq.scheduler.scheduler import TaskiqScheduler


@dataclass
class SchedulerArgs:
    """Arguments for scheduler."""

    scheduler: Union[str, TaskiqScheduler]
    modules: List[str]
    log_level: str = LogLevel.INFO.name
    configure_logging: bool = True
    fs_discover: bool = False
    tasks_pattern: str = "tasks.py"

    @classmethod
    def from_cli(cls, args: Optional[Sequence[str]] = None) -> "SchedulerArgs":
        """
        Build scheduler args from CLI arguments.

        This method takes arguments as args parameter.

        :param args: current CLI arguments, defaults to None
        :return: instance of scheduler args.
        """
        parser = ArgumentParser(
            formatter_class=ArgumentDefaultsHelpFormatter,
            description="Subcommand to run scheduler",
        )
        parser.add_argument("scheduler", help="Path to scheduler")
        parser.add_argument(
            "modules",
            help="List of modules where to look for tasks.",
            nargs=ZERO_OR_MORE,
        )
        parser.add_argument(
            "--fs-discover",
            "-fsd",
            action="store_true",
            help=(
                "If this option is on, "
                "taskiq will try to find tasks modules "
                "in current directory recursively. Name of file to search for "
                "can be configured using `--tasks-pattern` option."
            ),
        )
        parser.add_argument(
            "--tasks-pattern",
            "-tp",
            default="tasks.py",
            help="Name of files in which taskiq will try to find modules.",
        )
        parser.add_argument(
            "--log-level",
            default=LogLevel.INFO.name,
            choices=[level.name for level in LogLevel],
            help="scheduler log level",
        )
        parser.add_argument(
            "--no-configure-logging",
            action="store_false",
            dest="configure_logging",
            help="Use this parameter if your application configures custom logging.",
        )
        return cls(**parser.parse_args(args).__dict__)
