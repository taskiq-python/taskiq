import asyncio
from typing import AsyncGenerator, Generator

import pytest

from taskiq.dependencies import DependencyGraph, TaskiqDepends


@pytest.mark.anyio
async def test_dependency_successful() -> None:
    """Test that a simlpe dependencies work."""

    def dep1() -> int:
        return 1

    def testfunc(a: int = TaskiqDepends(dep1)) -> int:
        return a

    async with DependencyGraph(testfunc).ctx({}) as ctx:
        assert await ctx.resolve_kwargs() == {"a": 1}


@pytest.mark.anyio
async def test_dependency_async_successful() -> None:
    """Test that async dependencies work fine."""

    async def dep1() -> int:
        await asyncio.sleep(0.001)
        return 1

    def testfunc(a: int = TaskiqDepends(dep1)) -> int:
        return a

    async with DependencyGraph(testfunc).ctx({}) as ctx:
        assert await ctx.resolve_kwargs() == {"a": 1}


@pytest.mark.anyio
async def test_dependency_gen_successful() -> None:
    """Tests that generators work as expected."""
    starts = 0
    closes = 0

    def dep1() -> Generator[int, None, None]:
        nonlocal starts  # noqa: WPS420
        nonlocal closes  # noqa: WPS420

        starts += 1

        yield 1

        closes += 1

    def testfunc(a: int = TaskiqDepends(dep1)) -> int:
        return a

    async with DependencyGraph(testfunc).ctx({}) as ctx:
        assert await ctx.resolve_kwargs() == {"a": 1}
        assert starts == 1
        assert closes == 0
    assert closes == 1


@pytest.mark.anyio
async def test_dependency_async_gen_successful() -> None:
    """This test checks that async generators work."""
    starts = 0
    closes = 0

    async def dep1() -> AsyncGenerator[int, None]:
        nonlocal starts  # noqa: WPS420
        nonlocal closes  # noqa: WPS420

        await asyncio.sleep(0.001)
        starts += 1

        yield 1

        await asyncio.sleep(0.001)
        closes += 1

    def testfunc(a: int = TaskiqDepends(dep1)) -> int:
        return a

    async with DependencyGraph(testfunc).ctx({}) as ctx:
        assert await ctx.resolve_kwargs() == {"a": 1}
        assert starts == 1
        assert closes == 0
    assert closes == 1


@pytest.mark.anyio
async def test_dependency_subdeps() -> None:
    """Tests how subdependencies work."""

    def dep1() -> int:
        return 1

    def dep2(a: int = TaskiqDepends(dep1)) -> int:
        return a + 1

    def testfunc(a: int = TaskiqDepends(dep2)) -> int:
        return a

    async with DependencyGraph(testfunc).ctx({}) as ctx:
        assert await ctx.resolve_kwargs() == {"a": 2}


@pytest.mark.anyio
async def test_dependency_caches() -> None:
    """
    Tests how caches work.

    This test checks that
    if multiple functions depend on one function,
    This function must be calculated only once.
    """
    dep_exec = 0

    def dep1() -> int:
        nonlocal dep_exec  # noqa: WPS420
        dep_exec += 1

        return 1

    def dep2(a: int = TaskiqDepends(dep1)) -> int:
        return a + 1

    def dep3(a: int = TaskiqDepends(dep1)) -> int:
        return a + 1

    def testfunc(
        a: int = TaskiqDepends(dep2),
        b: int = TaskiqDepends(dep3),
    ) -> int:
        return a + b

    async with DependencyGraph(testfunc).ctx({}) as ctx:
        assert await ctx.resolve_kwargs() == {"a": 2, "b": 2}

    assert dep_exec == 1


@pytest.mark.anyio
async def test_dependency_subgraph() -> None:
    """
    Tests how subgraphs work.

    If use_cache is False it must force
    dependency graph to reevaluate it's subdependencies.
    """
    dep_exec = 0

    def dep1() -> int:
        nonlocal dep_exec  # noqa: WPS420
        dep_exec += 1

        return 1

    def dep2(a: int = TaskiqDepends(dep1)) -> int:
        return a + 1

    def dep3(a: int = TaskiqDepends(dep1, use_cache=False)) -> int:
        return a + 1

    def testfunc(
        a: int = TaskiqDepends(dep2),
        b: int = TaskiqDepends(dep3),
    ) -> int:
        return a + b

    async with DependencyGraph(testfunc).ctx({}) as ctx:
        assert await ctx.resolve_kwargs() == {"a": 2, "b": 2}

    assert dep_exec == 2
