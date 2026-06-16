import asyncio

from taskiq.receiver.batcher import Batcher


async def test_flush_on_size() -> None:
    flushed: list[list[int]] = []

    async def flush(name: str, items: list[int]) -> None:
        flushed.append(items)

    batcher = Batcher(flush)
    for i in range(3):
        await batcher.add("t", i, batch_size=3, batch_timeout=None)

    await asyncio.sleep(0)
    assert flushed == [[0, 1, 2]]


async def test_flush_on_timeout() -> None:
    flushed: list[list[int]] = []

    async def flush(name: str, items: list[int]) -> None:
        flushed.append(items)

    batcher = Batcher(flush)
    await batcher.add("t", 1, batch_size=None, batch_timeout=0.05)
    assert flushed == []
    await asyncio.sleep(0.1)
    assert flushed == [[1]]


async def test_size_cancels_timer() -> None:
    flushed: list[list[int]] = []

    async def flush(name: str, items: list[int]) -> None:
        flushed.append(items)

    batcher = Batcher(flush)
    await batcher.add("t", 1, batch_size=2, batch_timeout=10)
    await batcher.add("t", 2, batch_size=2, batch_timeout=10)
    await asyncio.sleep(0)
    assert flushed == [[1, 2]]
    await asyncio.sleep(0.05)
    assert flushed == [[1, 2]]


async def test_separate_buffers_per_task_name() -> None:
    flushed: list[tuple[str, list[int]]] = []

    async def flush(name: str, items: list[int]) -> None:
        flushed.append((name, items))

    batcher = Batcher(flush)
    await batcher.add("a", 1, batch_size=1, batch_timeout=None)
    await batcher.add("b", 2, batch_size=1, batch_timeout=None)
    await asyncio.sleep(0)
    assert ("a", [1]) in flushed
    assert ("b", [2]) in flushed


async def test_fresh_buffer_after_flush() -> None:
    flushed: list[list[int]] = []

    async def flush(name: str, items: list[int]) -> None:
        flushed.append(items)

    batcher = Batcher(flush)
    await batcher.add("t", 1, batch_size=1, batch_timeout=None)
    await batcher.add("t", 2, batch_size=1, batch_timeout=None)
    await asyncio.sleep(0)
    assert flushed == [[1], [2]]


async def test_flush_all_drains_buffers() -> None:
    flushed: list[list[int]] = []

    async def flush(name: str, items: list[int]) -> None:
        flushed.append(items)

    batcher = Batcher(flush)
    await batcher.add("t", 1, batch_size=10, batch_timeout=None)
    await batcher.flush_all()
    assert flushed == [[1]]


async def test_flush_all_empty_is_noop() -> None:
    flushed: list[list[int]] = []

    async def flush(name: str, items: list[int]) -> None:
        flushed.append(items)

    batcher = Batcher(flush)
    await batcher.flush_all()
    assert flushed == []
