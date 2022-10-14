import inspect
from typing import Any, Coroutine, TypeVar, Union

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
