import pytest

from taskiq.serializers.json_serializer import JSONSerializer


def test_json_dumpb() -> None:
    serizalizer = JSONSerializer()
    assert serizalizer.dumpb(None) == b"null"
    assert serizalizer.dumpb(1) == b"1"
    assert serizalizer.dumpb("a") == b'"a"'
    assert serizalizer.dumpb(["a"]) == b'["a"]'
    assert serizalizer.dumpb({"a": "b"}) == b'{"a": "b"}'


def test_json_loadb() -> None:
    serizalizer = JSONSerializer()
    assert serizalizer.loadb(b"null") is None
    assert serizalizer.loadb(b"1") == 1
    assert serizalizer.loadb(b'"a"') == "a"
    assert serizalizer.loadb(b'["a"]') == ["a"]
    assert serizalizer.loadb(b'{"a": "b"}') == {"a": "b"}


@pytest.mark.parametrize(
    ("ensure_ascii", "result"),
    [
        (True, b'"\\u043f\\u0440\\u0438\\u0432\\u0435\\u0442"'),
        (False, b'"\xd0\xbf\xd1\x80\xd0\xb8\xd0\xb2\xd0\xb5\xd1\x82"'),
    ],
)
def test_json_dumpb_with_ensure_ascii(ensure_ascii: bool, result: bytes) -> None:
    serizalizer = JSONSerializer(ensure_ascii=ensure_ascii)
    assert serizalizer.dumpb("привет") == result
