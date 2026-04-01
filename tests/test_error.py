import pytest

from taskiq.error import Error
from taskiq.exceptions import SecurityError, TaskiqResultTimeoutError


class SimpleError(Error):
    __template__ = "simple"


class ValueTemplateError(Error):
    __template__ = "value={value}"
    value: int


class DefaultValueTemplateError(Error):
    __template__ = "value={value}"
    value: int = 10


class BaseError(Error):
    __template__ = "base={base}, child={child}"
    base: int = 1


class ChildError(BaseError):
    child: str


class MissingAnnotationError(Error):
    __template__ = "value={value}"


class IndexedTemplateError(Error):
    __template__ = "{payload[key]}"
    payload: dict[str, str]


def test_simple_error_message_and_repr() -> None:
    error = SimpleError()
    assert str(error) == "simple"
    assert error.args == ("simple",)
    assert repr(error).endswith(".SimpleError()")


def test_template_with_required_value() -> None:
    error = ValueTemplateError(value=3)
    assert str(error) == "value=3"
    assert repr(error).endswith(".ValueTemplateError(value=3)")


def test_missing_argument_raises_type_error() -> None:
    with pytest.raises(TypeError, match="Missing arguments: 'value'"):
        ValueTemplateError()  # type: ignore[call-arg]


def test_undeclared_argument_raises_type_error() -> None:
    with pytest.raises(TypeError, match="Undeclared arguments: 'extra'"):
        ValueTemplateError(value=1, extra=2)  # type: ignore[call-arg]


def test_default_value_is_used_without_kwargs() -> None:
    error = DefaultValueTemplateError()
    assert str(error) == "value=10"
    assert repr(error).endswith(".DefaultValueTemplateError(value=10)")


def test_annotations_are_collected_from_inheritance() -> None:
    error = ChildError(child="ok")
    assert str(error) == "base=1, child=ok"
    assert repr(error).endswith(".ChildError(base=1, child='ok')")


def test_template_fields_must_be_annotated() -> None:
    with pytest.raises(ValueError, match="Fields must be annotated: 'value'"):
        MissingAnnotationError()


def test_indexed_template_field_does_not_require_extra_annotation() -> None:
    error = IndexedTemplateError(payload={"key": "value"})
    assert str(error) == "value"


def test_taskiq_exceptions_use_error_base_correctly() -> None:
    timeout_error = TaskiqResultTimeoutError(timeout=1.5)
    security_error = SecurityError(description="boom")
    assert str(timeout_error) == "Waiting for task results has timed out, timeout=1.5"
    assert str(security_error) == "Security exception occurred: boom"
