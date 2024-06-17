import inspect
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

_T = TypeVar("_T")


async def maybe_awaitable(
    possible_coroutine: "Union[_T, Coroutine[Any, Any, _T], Awaitable[_T]]",
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


def get_present_object_fields(
    obj: Any,
    fields: Sequence[str],
    check_condition: Optional[Callable[[Any, str], bool]] = None,
) -> List[str]:
    """
    Check the presence of the fields in the object.

    :param obj: Object to check fields in
    :param fields: Sequence of fields
    :param check_condition: A function to check the value is considered present
    :return: present fields.
    """
    if not check_condition:
        if isinstance(obj, dict):

            def check_condition(obj: Dict[str, Any], field: str) -> bool:
                return field in obj and obj[field] is not None

        else:

            def check_condition(obj: Any, field: str) -> bool:
                return getattr(obj, field, None) is not None

    present_fields = []
    for field in fields:
        try:
            if check_condition(obj, field):
                present_fields.append(field)
        except AttributeError:
            pass
    return present_fields
