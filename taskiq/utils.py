import asyncio
import contextlib
import inspect
from collections.abc import AsyncGenerator, Awaitable, Coroutine
from types import CoroutineType
from typing import Any, TypeVar, Union

_T = TypeVar("_T")


async def maybe_awaitable(
    possible_coroutine: Union[
        _T,
        Coroutine[Any, Any, _T],
        "CoroutineType[Any, Any, _T]",
        Awaitable[_T],
    ],
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


async def merge_async_iterables(
    *iterables: AsyncGenerator[_T, None],
) -> AsyncGenerator[_T, None]:
    """
    Merge multiple async generators into a single one.

    All values from every generator are yielded
    as soon as they arrive, in no particular order.

    If any of the inner generators raises an exception,
    this exception is re-raised for the consumer.

    When the merged generator is closed or cancelled,
    all inner generators are closed as well.

    :param iterables: async generators to merge.
    :return: single generator with values from all generators.
    """
    if not iterables:
        return

    queue: asyncio.Queue[Any] = asyncio.Queue()
    pumps = [
        asyncio.create_task(_pump_iterable(iterable, queue)) for iterable in iterables
    ]
    stopped_count = 0
    try:
        while stopped_count < len(iterables):
            value = await queue.get()
            if value is _MERGE_STOP:
                stopped_count += 1
                continue
            if isinstance(value, BaseException):
                raise value
            yield value
    finally:
        for pump_task in pumps:
            pump_task.cancel()
        await asyncio.gather(*pumps, return_exceptions=True)
        for iterable in iterables:
            # A generator is already closed if its pump was
            # cancelled in the middle of iteration. For others
            # this triggers their cleanup logic.
            with contextlib.suppress(RuntimeError):
                await iterable.aclose()


_MERGE_STOP = object()


async def _pump_iterable(
    iterable: AsyncGenerator[_T, None],
    queue: "asyncio.Queue[Any]",
) -> None:
    """Forward all values from a single iterable to the shared queue."""
    try:
        async for value in iterable:
            await queue.put(value)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:
        await queue.put(exc)
    else:
        await queue.put(_MERGE_STOP)
