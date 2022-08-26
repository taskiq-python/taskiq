import inspect
from dataclasses import dataclass
from typing import Any, Type

import pytest
from pydantic import BaseModel

from taskiq.cli.params_parser import parse_params
from taskiq.message import TaskiqMessage


class _TestPydanticClass(BaseModel):
    field: str


@dataclass
class _TestDataclass:
    field: str


def test_parse_params_no_signature() -> None:
    """Test that params aren't parsed if no annotation is supplied."""
    src_msg = TaskiqMessage(
        task_id="",
        task_name="",
        labels={},
        args=[1, 2],
        kwargs={"a": 1},
    )
    modify_msg = src_msg.copy(deep=True)
    parse_params(
        signature=None,
        message=modify_msg,
    )

    assert modify_msg == src_msg


@pytest.mark.parametrize("test_class", [_TestPydanticClass, _TestDataclass])
def test_parse_params_classes(test_class: Type[Any]) -> None:
    """Test that dataclasses are parsed correctly."""

    def test_func(param: test_class) -> test_class:  # type: ignore
        return param

    msg_with_args = TaskiqMessage(
        task_id="",
        task_name="",
        labels={},
        args=[{"field": "test_val"}],
        kwargs={},
    )

    parse_params(inspect.signature(test_func), msg_with_args)

    assert isinstance(msg_with_args.args[0], test_class)
    assert msg_with_args.args[0].field == "test_val"

    msg_with_kwargs = TaskiqMessage(
        task_id="",
        task_name="",
        labels={},
        args=[],
        kwargs={"param": {"field": "test_val"}},
    )

    parse_params(inspect.signature(test_func), msg_with_kwargs)

    assert isinstance(msg_with_kwargs.kwargs["param"], test_class)
    assert msg_with_kwargs.kwargs["param"].field == "test_val"


@pytest.mark.parametrize("test_class", [_TestPydanticClass, _TestDataclass])
def test_parse_params_wrong_data(test_class: Type[Any]) -> None:
    """Tests that wrong data isn't parsed and doesn't throw errors."""

    def test_func(param: test_class) -> test_class:  # type: ignore
        return param

    msg_with_args = TaskiqMessage(
        task_id="",
        task_name="",
        labels={},
        args=[{"unknown": "unknown"}],
        kwargs={},
    )

    parse_params(inspect.signature(test_func), msg_with_args)

    assert isinstance(msg_with_args.args[0], dict)

    msg_with_kwargs = TaskiqMessage(
        task_id="",
        task_name="",
        labels={},
        args=[],
        kwargs={"param": {"unknown": "unknown"}},
    )

    parse_params(inspect.signature(test_func), msg_with_kwargs)

    assert isinstance(msg_with_kwargs.kwargs["param"], dict)


@pytest.mark.parametrize("test_class", [_TestPydanticClass, _TestDataclass])
def test_parse_params_nones(test_class: Type[Any]) -> None:
    """Tests that None values are not parsed."""

    def test_func(param: test_class) -> test_class:  # type: ignore
        return param

    msg_with_args = TaskiqMessage(
        task_id="",
        task_name="",
        labels={},
        args=[None],
        kwargs={},
    )

    parse_params(inspect.signature(test_func), msg_with_args)

    assert msg_with_args.args[0] is None

    msg_with_kwargs = TaskiqMessage(
        task_id="",
        task_name="",
        labels={},
        args=[],
        kwargs={"param": None},
    )

    parse_params(inspect.signature(test_func), msg_with_kwargs)

    assert msg_with_kwargs.kwargs["param"] is None