import asyncio
from contextlib import suppress
from pathlib import Path
from unittest.mock import patch

import pytest

from taskiq.cli.utils import create_event_loop, import_tasks, resolve_loop_factory


def test_resolve_loop_factory_from_import_string() -> None:
    assert resolve_loop_factory("asyncio:new_event_loop") is asyncio.new_event_loop


def test_resolve_loop_factory_rejects_non_callable() -> None:
    with pytest.raises(ValueError, match="must be callable"):
        resolve_loop_factory("asyncio:ALL_COMPLETED")


def test_create_event_loop_rejects_invalid_result() -> None:
    factory = resolve_loop_factory("builtins:object")

    with pytest.raises(ValueError, match="must return an event loop"):
        create_event_loop(factory)


def test_import_tasks_list_pattern() -> None:
    modules = ["taskiq.tasks"]
    with patch("taskiq.cli.utils.import_from_modules", autospec=True) as mock:
        import_tasks(modules, ["tests/**/test_utils.py"], True)
        assert set(modules) == {
            "taskiq.tasks",
            "tests.test_utils",
            "tests.cli.test_utils",
        }
        mock.assert_called_with(modules)


def test_import_tasks_str_pattern() -> None:
    modules = ["taskiq.tasks"]
    with patch("taskiq.cli.utils.import_from_modules", autospec=True) as mock:
        import_tasks(modules, "tests/**/test_utils.py", True)
        assert set(modules) == {
            "taskiq.tasks",
            "tests.test_utils",
            "tests.cli.test_utils",
        }
        mock.assert_called_with(modules)


def test_import_tasks_empty_pattern() -> None:
    modules = ["taskiq.tasks"]
    with patch("taskiq.cli.utils.import_from_modules", autospec=True) as mock:
        import_tasks(modules, [], True)
        assert modules == ["taskiq.tasks"]
        mock.assert_called_with(modules)


def test_import_tasks_no_discover() -> None:
    modules = ["taskiq.tasks"]
    with patch("taskiq.cli.utils.import_from_modules", autospec=True) as mock:
        import_tasks(modules, "tests/**/test_utils.py", False)
        assert modules == ["taskiq.tasks"]
        mock.assert_called_with(modules)


def test_import_tasks_non_py_list_pattern() -> None:
    modules = ["taskiq.tasks"]
    with patch("taskiq.cli.utils.import_from_modules", autospec=True) as mock:
        paths = (
            Path("tests/test1.so"),
            Path("tests/cli/test2.cpython-313-darwin.so"),
        )
        for path in paths:
            path.touch()

        try:
            import_tasks(modules, ["tests/**/test_utils.py", "tests/**/*.so"], True)
            assert set(modules) == {
                "taskiq.tasks",
                "tests.test_utils",
                "tests.cli.test_utils",
                "tests.test1",
                "tests.cli.test2",
            }
            mock.assert_called_with(modules)
        finally:
            for path in paths:
                with suppress(FileNotFoundError):
                    path.unlink()
