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

    async def flush(name: str, items: list[int], shutdown: bool = False) -> None:
        flushed.append(items)

    batcher = Batcher(flush)
    await batcher.add("t", 1, batch_size=10, batch_timeout=None)
    await batcher.flush_all()
    assert flushed == [[1]]


async def test_flush_all_empty_is_noop() -> None:
    flushed: list[list[int]] = []

    async def flush(name: str, items: list[int], shutdown: bool = False) -> None:
        flushed.append(items)

    batcher = Batcher(flush)
    await batcher.flush_all()
    assert flushed == []


async def test_flush_all_non_closing_leaves_batcher_usable() -> None:
    """A non-closing drain (wait_all path) must keep the batcher operational.

    flush_all(closing=False) drains buffers like a test-time flush but must NOT
    permanently close the batcher: a subsequent ``add`` that arms a timer must
    still fire. It must also use the normal (non-shutdown) flush path, so the
    callback is never invoked with shutdown=True. Before the fix, flush_all sets
    _closing=True forever, so the second timer never fires (RED).
    """
    flushed: list[list[int]] = []
    shutdown_flags: list[bool] = []

    async def flush(name: str, items: list[int], shutdown: bool = False) -> None:
        flushed.append(items)
        shutdown_flags.append(shutdown)

    batcher = Batcher(flush)

    # First item, arm a timer, then drain via the non-closing path.
    await batcher.add("t", 1, batch_size=None, batch_timeout=0.01)
    await batcher.flush_all(closing=False)
    assert flushed == [[1]]
    assert batcher._closing is False
    # The non-closing drain must NOT have used the shutdown flush path.
    assert shutdown_flags == [False]

    # Now the batcher must still be usable: a new timer-armed add must fire.
    await batcher.add("t", 2, batch_size=None, batch_timeout=0.01)
    await asyncio.sleep(0.05)
    assert flushed == [[1], [2]]
    assert shutdown_flags == [False, False]


async def test_flush_all_default_closes_batcher() -> None:
    """Default flush_all (closing=True) keeps shutdown semantics: closes forever.

    After a default flush_all, _closing is True and _spawn_timer_flush no-ops.
    """
    flushed: list[list[int]] = []

    async def flush(name: str, items: list[int], shutdown: bool = False) -> None:
        flushed.append(items)

    batcher = Batcher(flush)
    await batcher.add("t", 1, batch_size=10, batch_timeout=None)
    await batcher.flush_all()
    assert flushed == [[1]]
    assert batcher._closing is True

    # Late timer spawns must be no-ops after close.
    batcher._spawn_timer_flush("t")
    assert batcher._timer_tasks == set()


async def test_flush_all_awaits_inflight_timer_tasks() -> None:
    """Shutdown must drain timer-spawned flushes, not leak them.

    A timer fires and spawns a flush task whose callback is slow. flush_all
    drains the buffer (a no-op since the timer already swapped the items) and
    must wait for the in-flight timer task to finish, leaving no pending timer
    tasks behind. Before the fix, flush_all ignores _timer_tasks entirely, so
    the in-flight timer flush is still pending when flush_all returns.
    """
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def flush(name: str, items: list[int], shutdown: bool = False) -> None:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()

    batcher = Batcher(flush)
    await batcher.add("t", 1, batch_size=None, batch_timeout=0.01)

    # Let the timer fire and spawn the flush task; it now blocks in `flush`.
    await asyncio.wait_for(started.wait(), timeout=1)
    assert len(batcher._timer_tasks) == 1

    # Let the blocked flush finish concurrently with the shutdown drain.
    release.set()
    await batcher.flush_all()

    # After shutdown, no timer task may remain pending and the single batch
    # must have flushed exactly once.
    assert not batcher._timer_tasks
    assert calls == 1
