import enum
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from dataclasses import dataclass
from typing import List, Optional, Sequence


class LogLevel(str, enum.Enum):  # noqa: WPS600
    """Different log levels."""

    INFO = "INFO"
    WARNING = "WARNING"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    FATAL = "FATAL"


@dataclass
class WorkerArgs:
    """Taskiq worker CLI arguments."""

    broker: str
    modules: List[str]
    tasks_pattern: str = "tasks.py"
    fs_discover: bool = False
    log_level: LogLevel = LogLevel.INFO
    workers: int = 2
    log_collector_format: str = (
        "[%(asctime)s][%(levelname)-7s][%(module)s:%(funcName)s:%(lineno)d] %(message)s"
    )
    max_threadpool_threads: int = 10
    no_parse: bool = False
    shutdown_timeout: float = 5
    reload: bool = False
    no_gitignore: bool = False

    @classmethod
    def from_cli(  # noqa: WPS213
        cls,
        args: Optional[Sequence[str]] = None,
    ) -> "WorkerArgs":
        """
        Construct TaskiqArgs instanc from CLI arguments.

        :param args: list of args as for cli.
        :return: TaskiqArgs instance.
        """
        parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            "broker",
            help=(
                "Where to search for broker. "
                "This string must be specified in "
                "'module.module:variable' format."
            ),
        )
        parser.add_argument(
            "--tasks-pattern",
            "-tp",
            default="tasks.py",
            help="Name of files in which taskiq will try to find modules.",
        )
        parser.add_argument(
            "modules",
            help="List of modules where to look for tasks.",
            nargs="*",
        )
        parser.add_argument(
            "--fs-discover",
            "-fsd",
            action="store_true",
            help=(
                "If this option is on, "
                "taskiq will try to find tasks modules "
                "in current directory recursievly. Name of file to search for "
                "can be configured using `--tasks-pattern` option."
            ),
        )
        parser.add_argument(
            "--log-level",
            default="INFO",
            choices=[level.name for level in LogLevel],
            help="worker log level",
        )
        parser.add_argument(
            "--workers",
            "-w",
            type=int,
            default=2,
            help="Number of worker child processes",
        )
        parser.add_argument(
            "--log-collector-format",
            "-lcf",
            type=str,
            default=(
                "[%(asctime)s]"
                "[%(levelname)-7s]"
                "[%(module)s:%(funcName)s:%(lineno)d] "
                "%(message)s"
            ),
            help="Format wich is used when collecting logs from function execution",
        )
        parser.add_argument(
            "--no-parse",
            action="store_true",
            help=(
                "If this parameter is on,"
                " taskiq doesn't parse incoming parameters "
                " with pydantic."
            ),
        )
        parser.add_argument(
            "--max-threadpool-threads",
            type=int,
            help="Maximum number of threads for executing sync functions.",
        )
        parser.add_argument(
            "--shutdown-timeout",
            type=float,
            default=5,
            help="Maximum amount of time for graceful broker's shutdown is seconds.",
        )
        parser.add_argument(
            "--reload",
            "-r",
            action="store_true",
            help="Reload workers if file is changed.",
        )
        parser.add_argument(
            "--do-not-use-gitignore",
            action="store_true",
            dest="no_gitignore",
            help="Do not use gitignore to check for updated files.",
        )

        namespace = parser.parse_args(args)
        return WorkerArgs(**namespace.__dict__)
