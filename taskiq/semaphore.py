import asyncio
import sys
from typing import TYPE_CHECKING, Any, Deque

from typing_extensions import Literal


class DequeSemaphore(asyncio.Semaphore):
    """Deque based Semaphore."""

    if TYPE_CHECKING:  # noqa: WPS604
        _loop: asyncio.BaseEventLoop
        _waiters: Deque[Any]
        _wakeup_scheduled: bool
        _get_loop: Any

    if sys.version_info[1] < 10:  # noqa: C901, WPS604

        async def acquire_first(self) -> Literal[True]:
            """
            Acquire as soon as possible. LIFO style.

            :param self: DequeSemaphore
            :raises BaseException: exception
            :return: true
            """
            while self._value < 0:
                fut = self._loop.create_future()
                self._waiters.appendleft(fut)

                try:
                    await fut
                except BaseException:  # noqa: WPS424
                    self._value += 1

                    fut.cancel()
                    if not self.locked() and not fut.cancelled():
                        self._wake_up_next()
                    raise

            return True

    else:

        async def acquire_first(self) -> Literal[True]:
            """
            Acquire as soon as possible. LIFO style.

            :param self: DequeSemaphore
            :raises asyncio.exceptions.CancelledError: exception
            :return: true
            """
            # _wakeup_scheduled is set if *another* task is scheduled to wakeup
            # but its acquire() is not resumed yet
            while self._wakeup_scheduled or self._value <= 0:
                fut = self._get_loop().create_future()
                self._waiters.appendleft(fut)
                try:
                    await fut
                    # reset _wakeup_scheduled *after* waiting for a future
                    self._wakeup_scheduled = False
                except asyncio.exceptions.CancelledError:
                    self._wake_up_next()
                    raise

            self._value -= 1
            return True
