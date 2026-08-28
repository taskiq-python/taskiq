import pytest

from taskiq.acks import AckController, AcknowledgeType, parse_acknowledge_type


async def test_ack_progress_can_be_reported_repeatedly() -> None:
    events: list[str] = []

    def ack_callback() -> None:
        events.append("ack")

    async def ack_progress_callback() -> None:
        events.append("progress")

    controller = AckController(ack_callback, ack_progress_callback)

    assert controller.is_ackable
    assert controller.is_ack_progressable
    await controller.ack_progress()
    await controller.ack_progress()
    assert not controller.is_acked
    await controller.ack()
    await controller.ack_progress()

    assert controller.is_acked
    assert events == ["progress", "progress", "ack"]


async def test_ack_progress_raises_when_unsupported() -> None:
    controller = AckController(lambda: None)

    assert not controller.is_ack_progressable
    with pytest.raises(RuntimeError, match="does not support ack progress"):
        await controller.ack_progress()


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
