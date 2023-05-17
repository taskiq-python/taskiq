import pickle

from taskiq import TaskiqResult


def test_json_error_serialization() -> None:
    try:
        raise ValueError("Omg", [1, 2, 3])
    except Exception as exc:
        error = exc

    task = TaskiqResult(is_err=False, return_value=1, execution_time=0, error=error)
    data: TaskiqResult[int] = TaskiqResult.parse_raw(task.json())

    assert data.error.args == task.error.args  # type: ignore
    assert type(data.error) == type(task.error)


def test_pickle_error_serialization() -> None:
    try:
        raise ValueError("Omg", [1, 2, 3])
    except Exception as exc:
        error = exc

    task = TaskiqResult(is_err=False, return_value=1, execution_time=0, error=error)
    data = pickle.loads(pickle.dumps(task))

    assert data.error.args == task.error.args  # type: ignore
    assert type(data.error) == type(task.error)
