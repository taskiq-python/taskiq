import inspect
from logging import getLogger
from typing import Any, Dict, Optional

from taskiq.compat import parse_obj_as
from taskiq.message import TaskiqMessage

logger = getLogger(__name__)


def parse_params(  # noqa: C901
    signature: Optional[inspect.Signature],
    type_hints: Dict[str, Any],
    message: TaskiqMessage,
) -> None:
    """
    Parses incoming parameters.

    This function uses signature to get
    expected types of parameters.

    If the parameter from TaskiqMessage
    has different type it will try to parse
    it. But if parsing fails this function
    doesn't modify incoming parameter.

    For example

    you have task like this:

    >>> def my_task(a: int) -> str
    >>>     ...

    If you will call my_task.kiq("11")

    You'll receive parsed 11 (int).
    But, if you call it with mytask.kiq("str"),
    you get the same value.

    If you want to skip parsing completely,
    you can pass --no-parse to worker,
    or you can make some of parameters untyped,
    or use Any.

    Why do we need type_hints separate with
    Signature. The reason is simple.
    If some variable doesn't have a type hint
    it won't be added in the dict of type hints.

    :param signature: original function's signature.
    :param type_hints: function's type hints.
    :param message: incoming message.
    """
    if signature is None:
        return
    argnum = -1
    # Iterate over function's params.
    for param_name in signature.parameters.keys():
        # If parameter doesn't have an annotation.
        annot = type_hints.get(param_name)
        if annot is None:
            continue
        # Increment argument numbers. This is
        # for positional arguments.
        argnum += 1
        # Value from incoming message.
        value = None
        logger.debug("Trying to parse %s as %s", param_name, annot)
        # Check if we have positional arguments in passed message.
        if argnum < len(message.args):
            # Get positional argument.
            value = message.args[argnum]
            if value is None:
                continue
            try:
                # trying to parse found value as in type annotation.
                message.args[argnum] = parse_obj_as(annot, value)
            except (ValueError, RuntimeError) as exc:
                logger.debug(exc, exc_info=True)
        else:
            # We try to get this parameter from kwargs.
            value = message.kwargs.get(param_name)
            if value is None:
                continue
            try:
                # trying to parse found value as in type annotation.
                message.kwargs[param_name] = parse_obj_as(annot, value)
            except (ValueError, RuntimeError) as exc:
                logger.debug(exc, exc_info=True)
