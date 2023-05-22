import asyncio
import collections
from typing import Any, Deque

from typing_extensions import Literal


class PrioritySemaphore:
    """
    Custom semaphore with prioerities.

    https://neopythonic.blogspot.com/2022/10/reasoning-about-asynciosemaphore.html
    """

    def __init__(self, value: int) -> None:
        self._value = value
        self._waiters: Deque[asyncio.Future[Any]] = collections.deque()
        self._waiters_first: Deque[asyncio.Future[Any]] = collections.deque()

        if self._value < 0:
            raise ValueError("Value should be >= 0")

    async def __aenter__(self) -> Literal[True]:
        return await self.acquire()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()

    def locked(self) -> bool:
        """
        Returns True if semaphore cannot be acquired immediately.

        :returns: true or false
        """
        return (
            self._value == 0
            or any(not waiter.cancelled() for waiter in (self._waiters or ()))
            or any(not waiter.cancelled() for waiter in (self._waiters_first or ()))
        )

    def release(self) -> None:
        """Release a semaphore, incrementing the internal counter by one.

        When it was zero on entry and another coroutine is waiting for it to
        become larger than zero again, wake up that coroutine.
        """
        self._value += 1
        self._wakeup_next()

    async def acquire(self, first: bool = False) -> Literal[True]:  # noqa: C901
        """
        Acquire a semaphore.

        :param first: acquire ASAP
        :raises asyncio.exceptions.CancelledError: task cancelled
        :returns: true
        """
        if not self.locked():
            # No need to wait as the semaphore is not locked
            # and no one is waiting
            self._value -= 1
            return True

        # if there are waiters or the semaphore is locked
        fut: asyncio.Future[Any] = asyncio.Future()

        if first:
            self._waiters_first.append(fut)
        else:
            self._waiters.append(fut)

        try:
            try:  # noqa: WPS501, WPS505
                await fut
            finally:
                if first:
                    self._waiters_first.remove(fut)
                else:
                    self._waiters.remove(fut)

        except asyncio.exceptions.CancelledError:
            if not fut.cancelled():
                self._value += 1
                self._wakeup_next()
            raise

        if self._value > 0:
            # This is required for strict FIFO ordering
            # otherwise it can cause starvation on the waiting tasks
            # The next loop iteration will wake up the task and switch
            self._wakeup_next()

        return True

    async def acquire_first(self) -> Literal[True]:
        """
        Acquire a semaphore ASAP.

        :returns: true
        """
        return await self.acquire(True)

    def _wakeup_next(self) -> None:
        if not self._waiters and not self._waiters_first:
            return

        for fut in self._waiters_first:
            if not fut.done():
                self._value -= 1
                fut.set_result(True)
                return

        for fut in self._waiters:
            if not fut.done():
                self._value -= 1
                fut.set_result(True)
                return
