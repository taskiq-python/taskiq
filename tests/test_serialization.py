import json
import pickle
import re
import traceback
from typing import Any

import pytest
from pydantic import ValidationError

import taskiq
from taskiq import serialization
from taskiq.exceptions import SecurityError
from taskiq.serialization import (
    ExceptionRepr,
    _UnpickleableExceptionWrapper,
    exception_to_python,
    prepare_exception,
    subclass_exception,
)


class WrapObject:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args


class ParamError(Exception):
    def __init__(self, param: Any) -> None:
        self.param = param


class ObjectException:
    class NestedError(Exception):
        pass


Unpickleable = subclass_exception(
    "Unpickleable",
    KeyError,
    "foo.module",
)
Impossible = subclass_exception(
    "Impossible",
    object,  # type: ignore
    "foo.module",
)
Lookalike = subclass_exception(
    "Lookalike",
    WrapObject,  # type: ignore
    "foo.module",
)


class ReprStrException:
    def __repr__(self) -> str:
        raise ValueError("Repr Exception")


class ReprException(ReprStrException):
    def __str__(self) -> str:
        return "123"


class UnrepresentableStr(str):
    def __repr__(self) -> str:
        raise ValueError("Repr Exception")


@pytest.mark.parametrize(
    ("obj", "repr"),
    [
        ["123", repr("123")],
        [int, repr(int)],
        [123, "123"],
        [ReprException(), "123"],
        [UnrepresentableStr("123123"), "123123"],
    ],
)
def test_representable(obj: Any, repr: str) -> None:
    assert serialization.safe_repr(obj=obj) == repr


def test_unrepresentable() -> None:
    obj = ReprStrException()
    repr = serialization.safe_repr(obj=obj)
    assert repr.startswith("<Unrepresentable {!r}".format(type(obj)))


def test_create_exception_cls() -> None:
    assert serialization.create_exception_cls("FooError", "m")
    assert serialization.create_exception_cls("FooError", "m", KeyError)


def test_json_py3() -> None:
    expected = (1, "<class 'object'>")
    actual = serialization.ensure_serializable([1, object], coder=json)
    assert expected == actual


def test_pickle() -> None:
    expected = (1, object)
    actual = serialization.ensure_serializable(expected, coder=pickle)
    assert expected == actual


def test_init() -> None:
    x = _UnpickleableExceptionWrapper("foo", "Bar", (10, lambda x: x))
    assert x.exc_args
    assert len(x.exc_args) == 2


def test_unpickleable() -> None:
    coder = pickle
    x = prepare_exception(Unpickleable(1, 2, "foo"), coder)
    assert isinstance(x, KeyError)
    y = exception_to_python(x)
    assert isinstance(y, KeyError)


def test_json_exception_arguments() -> None:
    coder = json
    x = prepare_exception(Exception(object), coder)
    assert x == ExceptionRepr(
        exc_message=serialization.ensure_serializable((object,), coder),
        exc_type=Exception.__name__,
        exc_module=Exception.__module__,
        exc_cause=None,
        exc_context=None,
        exc_suppress_context=False,
    )
    y = exception_to_python(x)
    assert isinstance(y, Exception)


def test_json_exception_nested() -> None:
    coder = json
    x = prepare_exception(ObjectException.NestedError("msg"), coder)
    assert x == ExceptionRepr(
        exc_message=("msg",),
        exc_type="ObjectException.NestedError",
        exc_module=ObjectException.NestedError.__module__,
        exc_cause=None,
        exc_context=None,
        exc_suppress_context=False,
    )
    y = exception_to_python(x)
    assert isinstance(y, ObjectException.NestedError)


def test_impossible() -> None:
    coder = pickle
    with pytest.raises(ValidationError):
        prepare_exception(Impossible(), coder)


def test_regular() -> None:
    coder = pickle
    x = prepare_exception(KeyError("baz"), coder)
    assert isinstance(x, KeyError)
    y = exception_to_python(x)
    assert isinstance(y, KeyError)


def test_unicode_message() -> None:
    coder = json
    message = "\u03ac"
    x = prepare_exception(Exception(message), coder)
    assert x == ExceptionRepr(
        exc_message=(message,),
        exc_type=Exception.__name__,
        exc_module=Exception.__module__,
        exc_cause=None,
        exc_context=None,
        exc_suppress_context=False,
    )


def test_pickle_infinite_loop() -> None:
    error = KeyError("bar")
    error.__cause__ = error
    error.__context__ = error
    error.__suppress_context__ = False
    x = prepare_exception(error, pickle)
    assert x == error


def test_json_infinite_loop() -> None:
    error = KeyError("bar")
    error.__cause__ = error
    error.__context__ = error
    error.__suppress_context__ = False
    x = prepare_exception(error, json)
    assert x == ExceptionRepr(
        exc_type="KeyError",
        exc_message=("bar",),
        exc_module="builtins",
    )


def test_unpickleable_exception_wrapper() -> None:
    class SubError(Exception):
        pass

    error = SubError(lambda x: "123")
    x = prepare_exception(error, pickle)
    assert isinstance(x, _UnpickleableExceptionWrapper)
    assert str(x) == serialization.safe_repr(error)
    y = exception_to_python(x)
    assert isinstance(y, Exception)
    assert y.__class__.__name__ == error.__class__.__name__


def test_exception_to_python_when_none() -> None:
    assert exception_to_python(None) is None


def test_not_an_exception_but_a_callable() -> None:
    x = {"exc_message": ("echo 1",), "exc_type": "system", "exc_module": "os"}

    with pytest.raises(
        SecurityError,
        match=re.escape(
            r"Expected an exception class, got os.system with payload ('echo 1',)",
        ),
    ):
        exception_to_python(x)  # type: ignore


def test_not_an_exception_but_another_object() -> None:
    x = {"exc_message": (), "exc_type": "object", "exc_module": "builtins"}

    with pytest.raises(
        SecurityError,
        match=re.escape(
            r"Expected an exception class, got builtins.object with payload ()",
        ),
    ):
        exception_to_python(x)  # type: ignore


def test_exception_to_python_when_attribute_exception() -> None:
    test_exception = {
        "exc_type": "AttributeDoesNotExist",
        "exc_module": "celery",
        "exc_message": ["Raise Custom Message"],
    }

    result_exc = exception_to_python(test_exception)  # type: ignore
    assert str(result_exc) == "Raise Custom Message"


def test_exception_to_python_when_no_module() -> None:
    test_exception = {
        "exc_type": "TestParamException",
        "exc_module": None,
        "exc_message": ["Raise Custom Message"],
    }

    result_exc = exception_to_python(test_exception)  # type: ignore
    assert isinstance(result_exc, Exception)
    assert result_exc.__module__ == serialization.__name__
    assert result_exc.__class__.__name__ == "TestParamException"
    assert str(result_exc) == "Raise Custom Message"


def test_exception_to_python_when_type_error() -> None:
    taskiq.TestParamException = ParamError  # type: ignore
    test_exception = {
        "exc_type": "TestParamException",
        "exc_module": "taskiq",
        "exc_message": [],
    }

    result_exc = exception_to_python(test_exception)  # type: ignore
    del taskiq.TestParamException  # type: ignore
    assert str(result_exc) == "<class 'tests.test_serialization.ParamError'>(())"


def test_json_context() -> None:
    error1 = ValueError("Context")
    ValueError("Cause")
    error3 = ValueError("Error")

    try:
        try:
            raise error1
        except Exception as exc:
            raise error3 from exc

    except Exception as exc:
        error = exc

    value = exception_to_python(prepare_exception(error, json))
    text = traceback.format_exception(
        type(value),  # type: ignore
        value,
        tb=value.__traceback__,  # type: ignore
    )

    assert (
        traceback.format_exception_only(type(error1), error1)[0] in text  # type: ignore
    )
    assert (
        traceback.format_exception_only(type(error3), error3)[0] in text  # type: ignore
    )


def test_json_cause() -> None:
    error1 = ValueError("Context")
    error2 = ValueError("Cause")
    error3 = ValueError("Error")

    try:
        try:
            raise error1
        except Exception:
            raise error3 from error2

    except Exception as exc:
        error = exc

    value = exception_to_python(prepare_exception(error, json))
    text = traceback.format_exception(
        type(value),  # type: ignore
        value,
        tb=value.__traceback__,  # type: ignore
    )

    assert (
        traceback.format_exception_only(type(error2), error2)[0] in text  # type: ignore
    )
    assert (
        traceback.format_exception_only(type(error3), error3)[0] in text  # type: ignore
    )


def test_pickle_context() -> None:
    error1 = ValueError("Context")
    ValueError("Cause")
    error3 = ValueError("Error")

    try:
        try:
            raise error1
        except Exception as exc:
            raise error3 from exc

    except Exception as exc:
        error = exc

    value = exception_to_python(prepare_exception(error, pickle))
    text = traceback.format_exception(
        type(value),  # type: ignore
        value,
        tb=value.__traceback__,  # type: ignore
    )

    assert (
        traceback.format_exception_only(type(error1), error1)[0] in text  # type: ignore
    )
    assert (
        traceback.format_exception_only(type(error3), error3)[0] in text  # type: ignore
    )


def test_pickle_cause() -> None:
    error1 = ValueError("Context")
    error2 = ValueError("Cause")
    error3 = ValueError("Error")

    try:
        try:
            raise error1
        except Exception:
            raise error3 from error2

    except Exception as exc:
        error = exc

    value = exception_to_python(prepare_exception(error, pickle))
    text = traceback.format_exception(
        type(value),  # type: ignore
        value,
        tb=value.__traceback__,  # type: ignore
    )

    assert (
        traceback.format_exception_only(type(error2), error2)[0] in text  # type: ignore
    )
    assert (
        traceback.format_exception_only(type(error3), error3)[0] in text  # type: ignore
    )
