import asyncio
import inspect
from typing import TYPE_CHECKING, Any, Coroutine, Generic, Tuple, TypeVar, Union

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


class PriorityQueue(asyncio.PriorityQueue, Generic[_T]):  # type: ignore
    """PriorityQueue based Queue."""

    async def put_first(self, item: _T) -> None:
        """
        Put item in Queue with highest priority.

        :param item: value to prepend
        """
        self.counter += 1
        await self.put((0, self.counter, item))

    async def put_last(self, item: _T) -> None:
        """
        Put item in Queue with lowest priority.

        :param item: value to append
        """
        self.counter += 1
        await self.put((1, self.counter, item))

    def _init(self, maxsize: int) -> None:
        super()._init(maxsize)
        self.counter = 0


if TYPE_CHECKING:  # pragma: no cover

    class PriorityQueue(  # type: ignore  # noqa: F811
        asyncio.PriorityQueue[Tuple[int, int, _T]],
        Generic[_T],
    ):
        """PriorityQueue based Queue."""

        async def put_first(self, item: _T) -> None:
            """
            Put item in Queue with highest priority.

            :param item: value to prepend
            """
            ...

        async def put_last(self, item: _T) -> None:
            """
            Put item in Queue with lowest priority.

            :param item: value to append
            """
            ...
