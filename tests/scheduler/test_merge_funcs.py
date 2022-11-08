from taskiq.scheduler.merge_functions import only_unique, preserve_all


def test_preserve_all() -> None:
    first = [1, 2, 3]
    second = [3, 4, 5]
    assert preserve_all(first, second) == [1, 2, 3, 3, 4, 5]  # type: ignore


def test_only_unique() -> None:
    first = [1, 2, 3]
    second = [3, 4, 5]
    assert only_unique(first, second) == [1, 2, 3, 4, 5]  # type: ignore
