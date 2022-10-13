import pytest

from taskiq.utils import maybe_awaitable


@pytest.mark.anyio
async def test_maybe_awaitable_coroutine() -> None:
    async def meme() -> int:
        return 1

    val: int = await maybe_awaitable(meme())
    assert val == 1


@pytest.mark.anyio
async def test_maybe_awaitable_sync() -> None:
    def meme() -> int:
        return 1

    assert await maybe_awaitable(meme()) == 1
