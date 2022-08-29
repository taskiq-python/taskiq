from pathlib import Path
from typing import Callable

from gitignore_parser import parse_gitignore
from watchdog.events import FileSystemEvent


class FileWatcher:  # pragma: no cover
    """Filewatcher that watchs for filesystem changes."""

    def __init__(
        self,
        callback: Callable[[], None],
        use_gitignore: bool = True,
    ) -> None:
        self.callback = callback
        self.gitignore = None
        gpath = Path("./.gitignore")
        if use_gitignore and gpath.exists():
            self.gitignore = parse_gitignore(gpath)

    def dispatch(self, event: FileSystemEvent) -> None:
        """
        React to event.

        This function checks wether we need to
        react to event and calls callback if we do.

        :param event: incoming fs event.
        """
        if event.is_directory:
            return
        if event.event_type == "closed":
            return
        if ".pytest_cache" in event.src_path:
            return
        if "__pycache__" in event.src_path:
            return
        if self.gitignore and self.gitignore(event.src_path):
            return
        self.callback()
