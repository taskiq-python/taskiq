from typing import get_type_hints

from taskiq.cli.worker.receiver import inject_context
from taskiq.context import Context
from taskiq.message import TaskiqMessage


def test_inject_context_success() -> None:
    """Test that context variable is injected as expected."""

    def func(param1: int, ctx: Context) -> int:
        return param1

    message = TaskiqMessage(
        task_id="",
        task_name="",
        labels={},
        args=[1],
        kwargs={},
    )

    inject_context(
        get_type_hints(func),
        message=message,
        broker=None,  # type: ignore
    )

    assert message.kwargs.get("ctx")
    assert isinstance(message.kwargs["ctx"], Context)


def test_inject_context_success_string_annotation() -> None:
    """
    Test that context variable is injected as expected.

    This test checks that if Context was provided as
    string, then everything is work as expected.
    """

    def func(param1: int, ctx: "Context") -> int:
        return param1

    message = TaskiqMessage(
        task_id="",
        task_name="",
        labels={},
        args=[1],
        kwargs={},
    )

    inject_context(
        get_type_hints(func),
        message=message,
        broker=None,  # type: ignore
    )

    assert message.kwargs.get("ctx")
    assert isinstance(message.kwargs["ctx"], Context)


def test_inject_context_no_typehint() -> None:
    """Test that context won't be injected in untyped parameter."""

    def func(param1: int, ctx) -> int:  # type: ignore
        return param1

    message = TaskiqMessage(
        task_id="",
        task_name="",
        labels={},
        args=[1],
        kwargs={},
    )

    inject_context(
        get_type_hints(func),
        message=message,
        broker=None,  # type: ignore
    )

    assert message.kwargs.get("ctx") is None


def test_inject_context_no_ctx_parameter() -> None:
    """
    Tests that injector won't raise an error.

    If the Context-typed parameter doesn't exist.
    """

    def func(param1: int) -> int:
        return param1

    message = TaskiqMessage(
        task_id="",
        task_name="",
        labels={},
        args=[1],
        kwargs={},
    )

    inject_context(
        get_type_hints(func),
        message=message,
        broker=None,  # type: ignore
    )

    assert not message.kwargs
