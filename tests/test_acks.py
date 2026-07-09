import pytest

from taskiq.acks import AcknowledgeType, parse_acknowledge_type


def test_parse_acknowledge_type_from_enum() -> None:
    assert (
        parse_acknowledge_type(AcknowledgeType.WHEN_RECEIVED)
        == AcknowledgeType.WHEN_RECEIVED
    )


def test_parse_acknowledge_type_from_string() -> None:
    assert parse_acknowledge_type("when_executed") == AcknowledgeType.WHEN_EXECUTED


def test_parse_manual_acknowledge_type_from_string() -> None:
    assert parse_acknowledge_type("manual") == AcknowledgeType.MANUAL


def test_parse_acknowledge_type_unknown_string() -> None:
    with pytest.raises(ValueError, match="Unknown acknowledge type value"):
        parse_acknowledge_type("when_save")


def test_parse_acknowledge_type_unsupported_value() -> None:
    with pytest.raises(ValueError, match="Unsupported acknowledge type"):
        parse_acknowledge_type(1)
