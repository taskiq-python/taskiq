from logging import getLogger
from pathlib import Path
from typing import Any, Callable

from gitignore_parser import parse_gitignore
from watchdog.events import FileSystemEvent

logger = getLogger("taskiq.worker")


class FileWatcher:  # pragma: no cover
    """Filewatcher that watches for filesystem changes."""

    def __init__(
        self,
        callback: Callable[..., None],
        path: Path,
        use_gitignore: bool = True,
        **callback_kwargs: Any,
    ) -> None:
        self.callback = callback
        self.gitignore = None
        gpath = path / ".gitignore"
        if use_gitignore and gpath.exists():
            self.gitignore = parse_gitignore(gpath)
        self.callback_kwargs = callback_kwargs

    def dispatch(self, event: FileSystemEvent) -> None:
        """
        React to event.

        This function checks whether we need to
        react to event and calls callback if we do.

        :param event: incoming fs event.
        """
        if event.is_directory:
            return
        if event.event_type in {"opened", "closed"}:
            return
        if ".git" in event.src_path:
            return
        try:
            if self.gitignore and self.gitignore(event.src_path):
                return
        except Exception as exc:
            logger.info(
                f"Cannot check path `{event.src_path}` in gitignore. Cause: {exc}",
            )
            return

        logger.debug(f"File changed. Event: {event}")
        self.callback(**self.callback_kwargs)
