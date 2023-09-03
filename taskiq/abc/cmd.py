from abc import ABC, abstractmethod
from typing import Optional, Sequence


class TaskiqCMD(ABC):  # pragma: no cover
    """Base class for new commands."""

    short_help = ""

    @abstractmethod
    def exec(self, args: Sequence[str]) -> Optional[int]:
        """
        Execute the command.

        :param args: CLI args.
        """
