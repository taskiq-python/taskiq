import os
import sys
from contextlib import contextmanager
from importlib import import_module
from logging import getLogger
from pathlib import Path
from typing import Any, Generator, List

from taskiq.utils import remove_suffix

logger = getLogger("taskiq.worker")


@contextmanager
def add_cwd_in_path() -> Generator[None, None, None]:
    """
    Adds current directory in python path.

    This context manager adds current directory in sys.path,
    so all python files are discoverable now, without installing
    current project.

    :yield: none
    """
    cwd = Path.cwd()
    if str(cwd) in sys.path:
        yield
    else:
        logger.debug(f"Inserting {cwd} in sys.path")
        sys.path.insert(0, str(cwd))
        try:
            yield
        finally:
            try:
                sys.path.remove(str(cwd))
            except ValueError:
                logger.warning(f"Cannot remove '{cwd}' from sys.path")


def import_object(object_spec: str) -> Any:
    """
    It parses python object spec and imports it.

    :param object_spec: string in format like `package.module:variable`
    :raises ValueError: if spec has unknown format.
    :returns: imported broker.
    """
    import_spec = object_spec.split(":")
    if len(import_spec) != 2:
        raise ValueError("You should provide object path in `module:variable` format.")
    with add_cwd_in_path():
        module = import_module(import_spec[0])
    return getattr(module, import_spec[1])


def import_from_modules(modules: List[str]) -> None:
    """
    Import all modules from modules variable.

    :param modules: list of modules.
    """
    for module in modules:
        try:
            logger.info(f"Importing tasks from module {module}")
            with add_cwd_in_path():
                import_module(module)
        except ImportError as err:
            logger.warning(f"Cannot import {module}. Cause:")
            logger.exception(err)


def import_tasks(modules: List[str], pattern: str, fs_discover: bool) -> None:
    """
    Import tasks modules.

    This function is used to
    import all tasks from modules.

    :param modules: list of modules to import.
    :param pattern: pattern of a file if fs_discover is True.
    :param fs_discover: If true it will try to import modules
        from filesystem.
    """
    if fs_discover:
        for path in Path().rglob(pattern):
            modules.append(
                remove_suffix(str(path), ".py").replace(os.path.sep, "."),
            )

    import_from_modules(modules)
