import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass
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
