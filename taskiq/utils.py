import asyncio
import contextlib
import inspect
import sys
from collections.abc import AsyncGenerator, Awaitable, Coroutine
from types import CoroutineType
from typing import Any, TypeVar, Union

import anyio
from anyio.streams.memory import MemoryObjectSendStream

if sys.version_info >= (3, 11):
    from builtins import BaseExceptionGroup
else:
    from exceptiongroup import BaseExceptionGroup

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


async def _start_producer(
    stream: AsyncGenerator[_T, None],
    send_stream: MemoryObjectSendStream[_T],
) -> None:
    """
    Forward all values of one stream to the shared send stream.

    The source generator is closed even if the producer
    was cancelled while waiting in `send()` and not inside
    the generator itself.
    """
    try:
        async with send_stream:
            async for val in stream:
                await send_stream.send(val)
    finally:
        await stream.aclose()


async def _run_producers(
    iterables: "tuple[AsyncGenerator[_T, None], ...]",
    send_stream: MemoryObjectSendStream[_T],
) -> None:
    """
    Pump every iterable into the send stream in a task group.

    This coroutine runs as a dedicated task, so its task group
    is always entered and exited in the same task, no matter
    which task iterates over or closes the merged generator.
    """
    async with send_stream, anyio.create_task_group() as tg:
        for iterable in iterables:
            tg.start_soon(_start_producer, iterable, send_stream.clone())
        # The original send stream is closed, so only clones remain.
        # When all of them are closed, the receive stream ends.
        send_stream.close()


async def merge_async_iterables(
    *iterables: AsyncGenerator[_T, None],
) -> AsyncGenerator[_T, None]:
    """
    Merge multiple async generators into a single one.

    All values from every generator are yielded
    as soon as they arrive, in no particular order.

    If any of the inner generators raises an exception,
    all other generators are cancelled and the error is
    re-raised for the consumer wrapped in an exception group.

    When the merged generator is closed or cancelled,
    all inner generators are closed as well.

    :param iterables: async generators to merge.
    :return: single generator with values from all generators.
    """
    send_stream, receive_stream = anyio.create_memory_object_stream[_T]()
    producers = asyncio.create_task(_run_producers(iterables, send_stream))
    try:
        async for message in receive_stream:
            yield message
        # The receive stream ended, so every producer finished.
        # If any of them failed, its error surfaces now.
        await producers
    finally:
        producers.cancel()
        with contextlib.suppress(asyncio.CancelledError, BaseExceptionGroup):
            await producers
        receive_stream.close()
