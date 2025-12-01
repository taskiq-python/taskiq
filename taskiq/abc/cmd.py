from abc import ABC, abstractmethod
from collections.abc import Sequence


class TaskiqCMD(ABC):  # pragma: no cover
    """Base class for new commands."""

    short_help = ""

    @abstractmethod
    def exec(self, args: Sequence[str]) -> int | None:
        """
        Execute the command.

        :param args: CLI args.
        """
