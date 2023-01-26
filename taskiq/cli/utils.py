import os
import sys
from contextlib import contextmanager
from importlib import import_module
from logging import getLogger
from pathlib import Path
from typing import Any, Generator, List

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
    cwd = os.getcwd()
    if cwd in sys.path:
        yield
    else:
        logger.debug(f"Inserting {cwd} in sys.path")
        sys.path.insert(0, cwd)
        try:
            yield
        finally:
            try:  # noqa: WPS505
                sys.path.remove(cwd)
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
        except ImportError:
            logger.warning(f"Cannot import {module}")


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
        for path in Path(".").rglob(pattern):
            modules.append(str(path).removesuffix(".py").replace("/", "."))

    import_from_modules(modules)
