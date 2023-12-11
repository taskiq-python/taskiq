from unittest.mock import patch

from taskiq.cli.utils import import_tasks


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
