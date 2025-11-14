from argparse import ArgumentParser
from collections.abc import Sequence

from taskiq.abc.cmd import TaskiqCMD


class MyCommand(TaskiqCMD):
    short_help = "Demo command"

    def exec(self, args: Sequence[str]) -> None:
        parser = ArgumentParser()
        parser.add_argument(
            "--test",
            dest="test",
            default="default",
            help="My test parameter.",
        )
        parsed = parser.parse_args(args)
        print(parsed)
