import argparse
import sys
from typing import Dict

from importlib_metadata import entry_points, version

from taskiq.abc.cmd import TaskiqCMD


def main() -> None:  # noqa: C901, WPS210  # pragma: no cover
    """
    Main entrypoint of the taskiq.

    This function collects all python entrypoints
    and assembles a final argument parser.

    All found entrypoints are used as subcommands.
    All arguments are passed to them as it was a normal
    call.
    """
    found_plugins = len(entry_points().select(group="taskiq_cli"))
    parser = argparse.ArgumentParser(
        description=f"""
        CLI for taskiq. Distributed task queue.

        This is a meta CLI. It searches for installed plugins
        using python entrypoints
        and passes all arguments to them.

        We found {found_plugins} installed plugins.
        """,
    )
    parser.add_argument(
        "-V",
        "--version",
        dest="version",
        action="store_true",
        help="print current taskiq version and exit",
    )
    subcommands: Dict[str, TaskiqCMD] = {}
    subparsers = parser.add_subparsers(
        title="Available subcommands",
        metavar="",
        dest="subcommand",
    )
    for entrypoint in entry_points().select(group="taskiq_cli"):
        try:
            cmd_class = entrypoint.load()
        except ImportError:
            print(f"Could not load {entrypoint.value}")  # noqa: WPS421
            continue
        if issubclass(cmd_class, TaskiqCMD):
            subparsers.add_parser(
                entrypoint.name,
                help=cmd_class.short_help,
                add_help=False,
            )
            subcommands[entrypoint.name] = cmd_class()

    args, _ = parser.parse_known_args()

    if args.version:
        print(version("taskiq"))  # noqa: WPS421
        return

    if args.subcommand is None:
        parser.print_help()
        return

    command = subcommands[args.subcommand]
    sys.argv.pop(0)
    command.exec(sys.argv[1:])


if __name__ == "__main__":  # pragma: no cover
    main()
