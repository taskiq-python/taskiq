import pickle
import json

import pytest

from taskiq import TaskiqResult


def test_json_serialization() -> None:
    task = TaskiqResult(is_err=False, return_value="some value", execution_time=0)
    data = json.loads(task.model_dump_json())
    assert data["return_value"] == task.return_value


def test_pickle_serialization() -> None:
    task = TaskiqResult(is_err=False, return_value="some value", execution_time=0)
    data: TaskiqResult[str] = pickle.loads(pickle.dumps(task))
    assert data.return_value == task.return_value


def test_json_error_serialization() -> None:
    try:
        raise ValueError("Omg", [1, 2, 3])
    except Exception as exc:
        error = exc

    task = TaskiqResult(is_err=False, return_value=1, execution_time=0, error=error)
    data = json.loads(task.model_dump_json())

    assert len(data["error"]["exc_message"]) == 2
    args = list(task.error.args) # type: ignore
    assert data["error"]["exc_message"][0] == args[0]  # type: ignore
    assert data["error"]["exc_message"][1] == args[1]  # type: ignore


def test_pickle_error_serialization() -> None:
    try:
        raise ValueError("Omg", [1, 2, 3])
    except Exception as exc:
        error = exc

    task = TaskiqResult(is_err=False, return_value=1, execution_time=0, error=error)
    data = pickle.loads(pickle.dumps(task))

    assert data.error.args == task.error.args  # type: ignore
    assert type(data.error) == type(task.error)


def test_result_raise_for_error_exc() -> None:
    error = ValueError("Error")
    res = TaskiqResult(is_err=True, return_value=None, execution_time=0, error=error)
    res = pickle.loads(pickle.dumps(res))

    with pytest.raises(ValueError) as exc:
        res.raise_for_error()

    assert exc.value is res.error


def test_result_raise_for_error_res() -> None:
    res = TaskiqResult(
        is_err=False,
        return_value="Some value",
        execution_time=0,
        error=None,
    )
    res = pickle.loads(pickle.dumps(res))
    res = res.raise_for_error()

    assert isinstance(res, TaskiqResult)
    assert res.return_value == "Some value"
