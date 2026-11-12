import asyncio
from collections.abc import AsyncGenerator
from typing import cast

import pytest

from taskiq.utils import maybe_awaitable, merge_async_iterables


async def test_maybe_awaitable_coroutine() -> None:
    async def meme() -> int:
        return 1

    val: int = await maybe_awaitable(meme())
    assert val == 1


async def test_maybe_awaitable_sync() -> None:
    def meme() -> int:
        return 1

    assert await maybe_awaitable(meme()) == 1


async def test_merge_async_iterables_merges_all_iterables() -> None:
    async def gen(*items: int) -> AsyncGenerator[int, None]:
        for item in items:
            await asyncio.sleep(0)
            yield item

    merged = [
        item
        async for item in merge_async_iterables(
            gen(1, 2, 3),
            gen(4, 5, 6),
        )
    ]

    assert sorted(merged) == [1, 2, 3, 4, 5, 6]


async def test_merge_async_iterables_without_iterables() -> None:
    # The TypeVar cannot be inferred from an empty argument list,
    # so the return type is provided explicitly.
    merged = cast("AsyncGenerator[int, None]", merge_async_iterables())
    assert [item async for item in merged] == []


async def test_merge_async_iterables_propagates_inner_errors() -> None:
    async def failing() -> AsyncGenerator[int, None]:
        yield 1
        raise ValueError("inner generator failed")

    with pytest.raises(ValueError, match="inner generator failed"):
        async for _ in merge_async_iterables(failing()):
            pass


async def test_merge_async_iterables_closes_inner_iterables() -> None:
    closed: list[str] = []

    def tracking(name: str) -> AsyncGenerator[int, None]:
        async def gen() -> AsyncGenerator[int, None]:
            try:
                while True:
                    yield 1
                    await asyncio.sleep(0)
            finally:
                closed.append(name)

        return gen()

    merged = merge_async_iterables(tracking("first"), tracking("second"))
    await merged.__anext__()
    await merged.aclose()

    assert sorted(closed) == ["first", "second"]
