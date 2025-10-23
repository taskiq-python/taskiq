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
    app_dir: Optional[str] = None
    log_level: LogLevel = LogLevel.INFO
    configure_logging: bool = True
    fs_discover: bool = False
    tasks_pattern: Sequence[str] = ("**/tasks.py",)
    skip_first_run: bool = False
    update_interval: Optional[int] = None

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
        parser.add_argument(
            "scheduler",
            help="Path to scheduler or scheduler factory function",
        )
        parser.add_argument(
            "modules",
            help="List of modules where to look for tasks.",
            nargs=ZERO_OR_MORE,
        )
        parser.add_argument(
            "--app-dir",
            "-d",
            default=None,
            help=(
                "Path to application directory. "
                "This path will be used to import tasks modules. "
                "If not specified, current working directory will be used."
            ),
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
            default=["**/tasks.py"],
            action="append",
            help="Glob patterns of files in which taskiq will try to find the tasks.",
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
        parser.add_argument(
            "--skip-first-run",
            action="store_true",
            dest="skip_first_run",
            help=(
                "Skip first run of scheduler. "
                "This option skips running tasks immediately after scheduler start."
            ),
        )
        parser.add_argument(
            "--update-interval",
            type=int,
            default=None,
            help=(
                "Interval in seconds to check for new tasks. "
                "If not specified, scheduler will run once a minute."
            ),
        )

        namespace = parser.parse_args(args)
        # If there are any patterns specified, remove default.
        # This is an argparse limitation.
        if len(namespace.tasks_pattern) > 1:
            namespace.tasks_pattern.pop(0)
        # Convert log_level string to LogLevel enum
        namespace.log_level = LogLevel[namespace.log_level]
        return cls(**namespace.__dict__)
