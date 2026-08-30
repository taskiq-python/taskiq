import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, get_type_hints

import pytest
from pydantic import BaseModel

from taskiq.message import TaskiqMessage
from taskiq.receiver.params_parser import parse_params


def _helper(f: Callable[..., Any], message: TaskiqMessage) -> None:
    sign = inspect.signature(f)
    hints = get_type_hints(f)
    parse_params(sign, hints, message)


def test_primitive_args_success() -> None:
    def func(a: int, b: int) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=["1", "2"],
        kwargs={},
    )
    _helper(func, msg)
    assert msg.args == [1, 2]
    assert msg.kwargs == {}


def test_dataclasses_args_success() -> None:
    @dataclass
    class TestObj:
        a: int
        b: int

    def func(a: TestObj) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=[{"a": "10", "b": "20"}],
        kwargs={},
    )
    _helper(func, msg)
    assert msg.args == [TestObj(a=10, b=20)]
    assert msg.kwargs == {}


def test_pydantic_args_success() -> None:
    class TestObj(BaseModel):
        a: int
        b: int

    def func(a: TestObj) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=[{"a": "10", "b": "20"}],
        kwargs={},
    )
    _helper(func, msg)
    assert msg.args == [TestObj(a=10, b=20)]
    assert msg.kwargs == {}


def test_primitive_args_failure(caplog: pytest.LogCaptureFixture) -> None:
    def func(a: int, b: int) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=["f3", "2"],
        kwargs={},
    )
    with caplog.at_level(logging.WARNING):
        _helper(func, msg)
        assert "Can't parse argument 0" in caplog.text
        assert msg.args == ["f3", 2]


def test_dataclasses_args_failure(caplog: pytest.LogCaptureFixture) -> None:
    @dataclass
    class TestObj:
        a: int
        b: int

    def func(a: TestObj) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=[{"a": "10", "b": "f3"}],
        kwargs={},
    )
    with caplog.at_level(logging.WARNING):
        _helper(func, msg)
        assert "Can't parse argument 0" in caplog.text
        assert msg.args == [{"a": "10", "b": "f3"}]


def test_pyndantic_args_failure(caplog: pytest.LogCaptureFixture) -> None:
    class TestObj(BaseModel):
        a: int
        b: int

    def func(a: TestObj) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=[{"a": "10", "b": "f3"}],
        kwargs={},
    )

    with caplog.at_level(logging.WARNING):
        _helper(func, msg)
        assert "Can't parse argument 0" in caplog.text
        assert msg.args == [{"a": "10", "b": "f3"}]


def test_kwargs_primitives_success() -> None:
    def func(a: int, b: int) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=[1],
        kwargs={"b": "2"},
    )
    _helper(func, msg)
    assert msg.args == [1]
    assert msg.kwargs == {"b": 2}


def test_kwargs_dataclasses_success() -> None:
    @dataclass
    class TestObj:
        a: int
        b: int

    def func(a: TestObj) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=[],
        kwargs={"a": {"a": "10", "b": "20"}},
    )
    _helper(func, msg)
    assert msg.args == []
    assert msg.kwargs == {"a": TestObj(a=10, b=20)}


def test_kwargs_pyndantic_success() -> None:
    class TestObj(BaseModel):
        a: int
        b: int

    def func(a: TestObj) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=[],
        kwargs={"a": {"a": "10", "b": "20"}},
    )
    _helper(func, msg)
    assert msg.args == []
    assert msg.kwargs == {"a": TestObj(a=10, b=20)}


def test_kwargs_primitives_failure(caplog: pytest.LogCaptureFixture) -> None:
    def func(a: int, b: int) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=[],
        kwargs={"a": "1", "b": "f3"},
    )
    with caplog.at_level(logging.WARNING):
        _helper(func, msg)
        assert "Can't parse argument b" in caplog.text
        assert msg.kwargs == {"a": 1, "b": "f3"}


def test_kwargs_dataclasses_failure(caplog: pytest.LogCaptureFixture) -> None:
    @dataclass
    class TestObj:
        a: int
        b: int

    def func(a: TestObj) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=[],
        kwargs={"a": {"a": "10", "b": "f3"}},
    )
    with caplog.at_level(logging.WARNING):
        _helper(func, msg)
        assert "Can't parse argument a" in caplog.text
        assert msg.kwargs == {"a": {"a": "10", "b": "f3"}}


def test_kwargs_pyndantic_failure(caplog: pytest.LogCaptureFixture) -> None:
    class TestObj(BaseModel):
        a: int
        b: int

    def func(a: TestObj) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=[],
        kwargs={"a": {"a": "10", "b": "f3"}},
    )
    with caplog.at_level(logging.WARNING):
        _helper(func, msg)
        assert "Can't parse argument a" in caplog.text
        assert msg.kwargs == {"a": {"a": "10", "b": "f3"}}


def test_unannotated_param_value_left_untouched() -> None:
    def func(request_id, count: int) -> None:  # type: ignore  # noqa: ANN001
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=["12345", 3],
        kwargs={},
    )
    _helper(func, msg)
    assert msg.args == ["12345", 3]
    assert isinstance(msg.args[0], str)


def test_annotated_param_parsed_after_unannotated() -> None:
    def func(request_id, count: int) -> None:  # type: ignore  # noqa: ANN001
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=["12345", "5"],
        kwargs={},
    )
    _helper(func, msg)
    assert msg.args == ["12345", 5]


def test_datetime_param_parsed_after_unannotated() -> None:
    def func(ts, when: datetime) -> None:  # type: ignore  # noqa: ANN001
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=["2026-08-30T10:00:00", "2026-08-30T10:00:00"],
        kwargs={},
    )
    _helper(func, msg)
    assert msg.args[0] == "2026-08-30T10:00:00"
    assert msg.args[1] == datetime(2026, 8, 30, 10, 0)


def test_invalid_annotated_value_warns_about_right_arg(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def func(ctx, count: int) -> None:  # type: ignore  # noqa: ANN001
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=["hello", "not-an-int"],
        kwargs={},
    )
    with caplog.at_level(logging.WARNING):
        _helper(func, msg)
        assert "Can't parse argument 1" in caplog.text
        assert msg.args == ["hello", "not-an-int"]


def test_varargs_all_elements_parsed() -> None:
    def func(*values: int) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=["1", "2", "3"],
        kwargs={},
    )
    _helper(func, msg)
    assert msg.args == [1, 2, 3]


def test_varargs_with_leading_arg() -> None:
    def func(a: int, *values: int) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=["1", "2", "3"],
        kwargs={},
    )
    _helper(func, msg)
    assert msg.args == [1, 2, 3]


def test_unannotated_varargs_untouched() -> None:
    def func(*values) -> None:  # type: ignore  # noqa: ANN002
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=["1", 2],
        kwargs={},
    )
    _helper(func, msg)
    assert msg.args == ["1", 2]


def test_kwonly_param_parsed() -> None:
    def func(a: int, *, b: int) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=["1"],
        kwargs={"b": "2"},
    )
    _helper(func, msg)
    assert msg.args == [1]
    assert msg.kwargs == {"b": 2}


def test_positional_params_parsed_from_kwargs() -> None:
    def func(a: int, b: int) -> None:
        pass

    msg = TaskiqMessage(
        task_id="test",
        task_name="test",
        labels={},
        labels_types={},
        args=[],
        kwargs={"a": "1", "b": "2"},
    )
    _helper(func, msg)
    assert msg.kwargs == {"a": 1, "b": 2}
