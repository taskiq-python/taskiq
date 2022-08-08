import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Awaitable, Coroutine, TypeVar, Union

_T = TypeVar("_T")  # noqa: WPS111


def run_sync(coroutine: "Coroutine[Any, Any, _T]") -> _T:
    """
    Run the coroutine synchronously.

    This function tries to run corouting using asyncio.run.

    If it's not possible, it manually creates executor and
    runs async function returns it's result.

     1. When called within a coroutine.
     2. When called from ``python -m asyncio``, or iPython with %autoawait
        enabled, which means an event loop may already be running in the
        current thread.

    :param coroutine: awaitable to execute.
    :returns: the same type as if it were awaited.
    """
    try:
        # We try this first, as in most situations this will work.
        return asyncio.run(coroutine)
    except RuntimeError:
        # An event loop already exists.
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(asyncio.run, coroutine)
            return future.result()


async def maybe_awaitable(
    possible_coroutine: "Union[_T, Awaitable[_T]]",
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
