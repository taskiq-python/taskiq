import datetime
import uuid
from typing import Any

import pytest

from taskiq.abc.serializer import TaskiqSerializer
from taskiq.serializers import (
    CBORSerializer,
    JSONSerializer,
    MSGPackSerializer,
    ORJSONSerializer,
    PickleSerializer,
)


@pytest.mark.parametrize(
    "serializer",
    [
        JSONSerializer(),
        ORJSONSerializer(),
        CBORSerializer(),
        MSGPackSerializer(),
        PickleSerializer(),
    ],
)
@pytest.mark.parametrize(
    "data",
    [
        None,
        1,
        "a",
        ["a"],
        {"a": "b"},
        1.3,
    ],
)
def test_generic_serializer(serializer: TaskiqSerializer, data: Any) -> None:
    assert serializer.loadb(serializer.dumpb(data)) == data


@pytest.mark.parametrize(
    "serializer",
    [
        ORJSONSerializer(),
        CBORSerializer(),
        PickleSerializer(),
    ],
)
def test_uuid_serialization(serializer: TaskiqSerializer) -> None:
    data = uuid.uuid4()
    assert str(serializer.loadb(serializer.dumpb(data))) == str(data)


@pytest.mark.parametrize(
    "serializer",
    [
        CBORSerializer(),
        MSGPackSerializer(),
        PickleSerializer(),
    ],
)
def test_datetime_serialization(serializer: TaskiqSerializer) -> None:
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    assert serializer.loadb(serializer.dumpb(now)) == now
