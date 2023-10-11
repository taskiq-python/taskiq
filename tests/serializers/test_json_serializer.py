import pytest

from taskiq.serializers.json_serializer import JSONSerializer


@pytest.mark.anyio
async def test_json_dumpb() -> None:
    serizalizer = JSONSerializer()
    assert serizalizer.dumpb(None) == b"null"  # noqa: PLR2004
    assert serizalizer.dumpb(1) == b"1"  # noqa: PLR2004
    assert serizalizer.dumpb("a") == b'"a"'  # noqa: PLR2004
    assert serizalizer.dumpb(["a"]) == b'["a"]'  # noqa: PLR2004
    assert serizalizer.dumpb({"a": "b"}) == b'{"a": "b"}'  # noqa: PLR2004


@pytest.mark.anyio
async def test_json_loadb() -> None:
    serizalizer = JSONSerializer()
    assert serizalizer.loadb(b"null") is None
    assert serizalizer.loadb(b"1") == 1
    assert serizalizer.loadb(b'"a"') == "a"
    assert serizalizer.loadb(b'["a"]') == ["a"]
    assert serizalizer.loadb(b'{"a": "b"}') == {"a": "b"}
