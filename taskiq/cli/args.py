import enum
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from dataclasses import dataclass


class LogLevel(str, enum.Enum):  # noqa: WPS600
    """Different log levels."""

    INFO = "INFO"
    WARNING = "WARNING"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    FATAL = "FATAL"


@dataclass
class TaskiqArgs:
    """Taskiq worker CLI arguments."""

    broker: str
    tasks_pattern: str
    modules: list[str]
    fs_discover: bool
    log_level: str
    workers: int
    log_collector_format: str

    @classmethod
    def from_cli(cls) -> "TaskiqArgs":
        """
        Construct TaskiqArgs instanc from CLI arguments.

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
                "|%(asctime)s"
                "|%(levelname)8s"
                "|%(module)s:%(funcName)s:%(lineno)d| :: "
                "%(message)s"
            ),
            help="Format wich is used when collecting logs from function execution",
        )

        namespace = parser.parse_args()
        return TaskiqArgs(**namespace.__dict__)
