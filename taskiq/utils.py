import asyncio
import inspect
from typing import TYPE_CHECKING, Any, Coroutine, Deque, Generic, TypeVar, Union

from typing_extensions import Literal

_T = TypeVar("_T")  # noqa: WPS111


async def maybe_awaitable(
    possible_coroutine: "Union[_T, Coroutine[Any, Any, _T]]",
) -> _T:
    """
    Awaits coroutine if needed.

    This function allows run function
    that may return coroutine.

    It not awaitable value passed, it
    returned immediately.

    :param possible_coroutine: some value.
    :return: value.
    """
    if inspect.isawaitable(possible_coroutine):
        return await possible_coroutine
    return possible_coroutine  # type: ignore


def remove_suffix(text: str, suffix: str) -> str:
    """
    Removing a Suffix from a String with a Custom Function.

    :param text: String
    :param suffix: Removing a Suffix
    :return: value.
    """
    if text.endswith(suffix):
        return text[: -len(suffix)]
    return text


class DequeSemaphore(asyncio.Semaphore):
    """Deque based Semaphore."""

    if TYPE_CHECKING:  # noqa: WPS604
        _loop: asyncio.BaseEventLoop

    async def acquire_first(self) -> Literal[True]:
        """
        Acquire as soon as possible. LIFO style.

        :raises BaseException: exception
        :return: true
        """
        self._value -= 1

        while self._value < 0:
            fut: asyncio.Future[Any] = self._loop.create_future()
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


class DequeQueue(
    asyncio.Queue,  # type: ignore
    Generic[_T],
):
    """Deque based Queue."""

    if TYPE_CHECKING:  # noqa: WPS604
        _loop: asyncio.BaseEventLoop
        _queue: Deque[_T]
        _putters: Deque[Any]
        _getters: Deque[Any]
        _unfinished_tasks: int
        _finished: asyncio.Event
        _wakeup_next: Any

    async def put_first(self, item: _T) -> None:
        """
        Wait till queue is not full. Put item in Queue as soon as possible. LIFO style.

        :param item: value to prepend
        :raises BaseException: something goes wrong
        :returns: nothing
        """
        while self.full():
            putter = self._loop.create_future()
            self._putters.appendleft(putter)
            try:
                await putter
            except BaseException:  # noqa: WPS424
                putter.cancel()  # Just in case putter is not done yet.
                try:  # noqa: WPS505
                    # Clean self._putters from canceled putters.
                    self._putters.remove(putter)
                except ValueError:
                    # The putter could be removed from self._putters by a
                    # previous get_nowait call.
                    pass  # noqa: WPS420
                if not self.full() and not putter.cancelled():
                    # We were woken up by get_nowait(), but can't take
                    # the call.  Wake up the next in line.
                    self._wakeup_next(self._putters)
                raise

        return self.put_first_nowait(item)

    def put_first_nowait(self, item: _T) -> None:
        """
        Put item in Queue as soon as possible. LIFO style.

        :param item: value to prepend
        :raises QueueFull: queue is full
        """
        if self.full():
            raise asyncio.QueueFull()

        self._put_first(item)
        self._unfinished_tasks += 1
        self._finished.clear()
        self._wakeup_next(self._getters)

    def _put_first(self, item: _T) -> None:
        """
        Prepend item.

        :param item: value to prepend
        """
        self._queue.appendleft(item)
