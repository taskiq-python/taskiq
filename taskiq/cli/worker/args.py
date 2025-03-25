from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from dataclasses import dataclass, field
from typing import List, Optional, Sequence, Tuple

from taskiq.acks import AcknowledgeType
from taskiq.cli.common_args import LogLevel


def receiver_arg_type(string: str) -> Tuple[str, str]:
    """
    Parse cli --receiver_arg argument value.

    :param string: cli argument value in format key=value.
    :raises ValueError: if value not in format.
    :return: (key, value) pair.
    """
    args = string.split("=", 1)
    if len(args) != 2:
        raise ValueError(f"Invalid value: {string}")
    return args[0], args[1]


@dataclass
class WorkerArgs:
    """Taskiq worker CLI arguments."""

    broker: str
    modules: List[str]
    tasks_pattern: Sequence[str] = ("**/tasks.py",)
    fs_discover: bool = False
    configure_logging: bool = True
    log_level: LogLevel = LogLevel.INFO
    log_format: str = "[%(asctime)s][%(name)s][%(levelname)-7s][%(processName)s] %(message)s"
    workers: int = 2
    max_threadpool_threads: Optional[int] = None
    max_process_pool_processes: Optional[int] = None
    no_parse: bool = False
    shutdown_timeout: float = 5
    reload: bool = False
    no_gitignore: bool = False
    max_async_tasks: int = 100
    receiver: str = "taskiq.receiver:Receiver"
    receiver_arg: List[Tuple[str, str]] = field(default_factory=list)
    max_prefetch: int = 0
    no_propagate_errors: bool = False
    max_fails: int = -1
    ack_type: AcknowledgeType = AcknowledgeType.WHEN_SAVED
    max_tasks_per_child: Optional[int] = None
    wait_tasks_timeout: Optional[float] = None
    hardkill_count: int = 3
    use_process_pool: bool = False

    @classmethod
    def from_cli(
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
                "Where to search for broker or broker factory function. "
                "This string must be specified in "
                "'module.module:variable' format."
            ),
        )
        parser.add_argument(
            "--receiver",
            default="taskiq.receiver:Receiver",
            help=(
                "Where to search for receiver. "
                "This string must be specified in "
                "'module.module:variable' format."
            ),
        )
        parser.add_argument(
            "--receiver_arg",
            action="append",
            type=receiver_arg_type,
            default=[],
            help=(
                "List of args for receiver. "
                "This string must be specified in "
                "`key=value` format."
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
                "in current directory recursively. Name of file to search for "
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
            "--log-format",
            default="[%(asctime)s][%(name)s][%(levelname)-7s][%(processName)s] %(message)s",
            help="worker log format",
        )
        parser.add_argument(
            "--workers",
            "-w",
            type=int,
            default=2,
            help="Number of worker child processes",
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
            "--no-propagate-errors",
            action="store_true",
            dest="no_propagate_errors",
            help=(
                "If this parameter is on,"
                " all errors that happen in tasks "
                " won't be propagated to generator dependencies."
            ),
        )
        parser.add_argument(
            "--max-threadpool-threads",
            type=int,
            default=None,
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
            help="Reload workers if file is changed. "
            "`reload` extra is required for this option.",
        )
        parser.add_argument(
            "--do-not-use-gitignore",
            action="store_true",
            dest="no_gitignore",
            help="Do not use gitignore to check for updated files.",
        )
        parser.add_argument(
            "--max-async-tasks",
            type=int,
            dest="max_async_tasks",
            default=100,
            help="Maximum simultaneous async tasks per worker process. ",
        )
        parser.add_argument(
            "--max-prefetch",
            type=int,
            dest="max_prefetch",
            default=0,
            help="Maximum prefetched tasks per worker process. ",
        )
        parser.add_argument(
            "--no-configure-logging",
            action="store_false",
            dest="configure_logging",
            help="Use this parameter if your application configures custom logging.",
        )
        parser.add_argument(
            "--max-fails",
            type=int,
            dest="max_fails",
            default=-1,
            help="Maximum number of child process exits.",
        )
        parser.add_argument(
            "--ack-type",
            type=lambda value: AcknowledgeType(value.lower()),
            default=AcknowledgeType.WHEN_SAVED,
            choices=[ack_type.name.lower() for ack_type in AcknowledgeType],
            help="When to acknowledge message.",
        )
        parser.add_argument(
            "--max-tasks-per-child",
            type=int,
            default=None,
            help="Maximum number of tasks to execute per child process.",
        )
        parser.add_argument(
            "--wait-tasks-timeout",
            type=float,
            default=None,
            help="Maximum time to wait for all current tasks to finish before exiting.",
        )
        parser.add_argument(
            "--hardkill-count",
            type=int,
            default=3,
            help="Number of termination signals to the main "
            "process before performing a hardkill.",
        )
        parser.add_argument(
            "--use-process-pool",
            action="store_true",
            dest="use_process_pool",
            help="Use process pool instead of thread pool for sync tasks.",
        )
        parser.add_argument(
            "--max-process-pool-processes",
            type=int,
            dest="max_process_pool_processes",
            default=None,
            help="Maximum number of processes in process pool.",
        )

        namespace = parser.parse_args(args)
        # If there are any patterns specified, remove default.
        # This is an argparse limitation.
        if len(namespace.tasks_pattern) > 1:
            namespace.tasks_pattern.pop(0)
        return WorkerArgs(**namespace.__dict__)
