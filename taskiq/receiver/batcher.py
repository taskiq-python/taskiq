import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any

logger = getLogger(__name__)

FlushCallback = Callable[..., Awaitable[None]]


@dataclass
class _Buffer:
    items: list[Any] = field(default_factory=list)
    timer: asyncio.TimerHandle | None = None


class Batcher:
    """
    Buffers items per task name and flushes them on size or timeout.

    The first item arms the timeout timer; reaching ``batch_size`` flushes
    immediately. ``flush_all`` drains everything (used on shutdown).
    """

    def __init__(self, flush_cb: FlushCallback) -> None:
        self._flush_cb = flush_cb
        self._buffers: dict[str, _Buffer] = {}
        # Kept so timer tasks aren't GC'd mid-flight and their errors surface.
        self._timer_tasks: set[asyncio.Task[None]] = set()
        # When True, new timer flushes are not spawned (set on shutdown).
        self._closing = False

    async def add(
        self,
        task_name: str,
        item: Any,
        batch_size: int | None,
        batch_timeout: float | None,
    ) -> None:
        """
        Buffer one item, flushing the batch if size or timeout triggers.

        :param task_name: name of the batched task.
        :param item: item to buffer (opaque to the batcher).
        :param batch_size: flush once the buffer reaches this size.
        :param batch_timeout: flush this many seconds after the first item.
        """
        buf = self._buffers.get(task_name)
        if buf is None:
            buf = _Buffer()
            self._buffers[task_name] = buf

        was_empty = not buf.items
        buf.items.append(item)

        if was_empty and batch_timeout is not None:
            loop = asyncio.get_running_loop()
            buf.timer = loop.call_later(
                batch_timeout,
                self._spawn_timer_flush,
                task_name,
            )

        if batch_size is not None and len(buf.items) >= batch_size:
            await self._flush(task_name)

    def _spawn_timer_flush(self, task_name: str) -> None:
        """
        Run the timeout-driven flush as a tracked task.

        Kept in ``_timer_tasks`` so the flush isn't GC'd and its errors are
        logged instead of swallowed by the loop.

        :param task_name: name of the batched task to flush.
        """
        if self._closing:
            return
        task = asyncio.ensure_future(self._flush(task_name))
        self._timer_tasks.add(task)

        def _done(finished: "asyncio.Task[None]") -> None:
            self._timer_tasks.discard(finished)
            if not finished.cancelled():
                exc = finished.exception()
                if exc is not None:
                    logger.error(
                        "Timeout flush for batched task %s failed: %s",
                        task_name,
                        exc,
                        exc_info=exc,
                    )

        task.add_done_callback(_done)

    async def flush_all(self, closing: bool = True) -> None:
        """
        Drain every buffer and wait for in-flight timer flushes.

        With ``closing=True`` (shutdown, the default) the batcher is closed for
        good: no new timer flushes are spawned and buffers drain via the
        shutdown path. With ``closing=False`` (the ``wait_all`` test drain) the
        batcher stays usable afterwards, so later ``add`` calls can re-arm
        timers.

        :param closing: permanently close the batcher (shutdown).
        """
        if closing:
            self._closing = True
        # Cancel timers that haven't fired yet; we flush their buffers now.
        for buf in self._buffers.values():
            if buf.timer is not None:
                buf.timer.cancel()
                buf.timer = None
        for task_name in list(self._buffers):
            await self._flush(task_name, shutdown=closing)
        # Wait for timer flushes that were already in flight.
        if self._timer_tasks:
            await asyncio.gather(*self._timer_tasks, return_exceptions=True)

    async def _flush(self, task_name: str, shutdown: bool = False) -> None:
        buf = self._buffers.get(task_name)
        if buf is None or not buf.items:
            return
        if buf.timer is not None:
            buf.timer.cancel()
            buf.timer = None
        items = buf.items
        buf.items = []
        if shutdown:
            await self._flush_cb(task_name, items, shutdown=True)
        else:
            await self._flush_cb(task_name, items)
