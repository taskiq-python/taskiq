import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any

logger = getLogger(__name__)

FlushCallback = Callable[[str, list[Any]], Awaitable[None]]


@dataclass
class _Buffer:
    items: list[Any] = field(default_factory=list)
    timer: asyncio.TimerHandle | None = None


class Batcher:
    """
    Accumulates items per task name and flushes them on size or timeout.

    The first item added to an empty buffer arms the timeout timer (when a
    timeout is configured). Reaching ``batch_size`` flushes immediately and
    cancels the pending timer. ``flush_all`` drains every buffer and is used
    on shutdown so buffered items are not lost.
    """

    def __init__(self, flush_cb: FlushCallback) -> None:
        self._flush_cb = flush_cb
        self._buffers: dict[str, _Buffer] = {}
        # Tasks spawned by timeout timers. Kept so they aren't garbage
        # collected mid-flight and so their errors are retrieved/logged.
        self._timer_tasks: set[asyncio.Task[None]] = set()

    async def add(
        self,
        task_name: str,
        item: Any,
        batch_size: int | None,
        batch_timeout: float | None,
    ) -> None:
        """
        Add one item to the named buffer, flushing if a trigger fires.

        :param task_name: name of the batched task.
        :param item: item to buffer (opaque to the batcher).
        :param batch_size: flush when the buffer reaches this size.
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
        Spawn the timeout-driven flush as a tracked task.

        The task is retained until it finishes and its result is retrieved in
        a done-callback so timeout-flush errors are logged instead of being
        silently swallowed by the event loop.

        :param task_name: name of the batched task to flush.
        """
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

    async def flush_all(self) -> None:
        """Flush every non-empty buffer."""
        for task_name in list(self._buffers):
            await self._flush(task_name)

    async def _flush(self, task_name: str) -> None:
        buf = self._buffers.get(task_name)
        if buf is None or not buf.items:
            return
        if buf.timer is not None:
            buf.timer.cancel()
            buf.timer = None
        items = buf.items
        buf.items = []
        await self._flush_cb(task_name, items)
