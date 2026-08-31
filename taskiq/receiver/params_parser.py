import inspect
from logging import getLogger
from typing import Any

from taskiq.compat import parse_obj_as
from taskiq.message import TaskiqMessage

logger = getLogger(__name__)


def _parse_arg(
    param_name: str,
    annot: Any,
    argnum: int,
    message: TaskiqMessage,
) -> None:
    """
    Parse a positional argument by its annotation, in place.

    :param param_name: name of the parameter.
    :param annot: type annotation of the parameter.
    :param argnum: index of the argument in message.args.
    :param message: incoming message.
    """
    value = message.args[argnum]
    if value is None:
        return
    logger.debug("Trying to parse %s as %s", param_name, annot)
    try:
        # trying to parse found value as in type annotation.
        message.args[argnum] = parse_obj_as(annot, value)
    except (ValueError, RuntimeError) as exc:
        logger.warning(
            "Can't parse argument %d for task %s. Reason: %s",
            argnum,
            message.task_name,
            exc,
            exc_info=True,
        )


def _parse_kwarg(param_name: str, annot: Any, message: TaskiqMessage) -> None:
    """
    Parse a keyword argument by its annotation, in place.

    :param param_name: name of the parameter.
    :param annot: type annotation of the parameter.
    :param message: incoming message.
    """
    value = message.kwargs.get(param_name)
    if value is None:
        return
    logger.debug("Trying to parse %s as %s", param_name, annot)
    try:
        # trying to parse found value as in type annotation.
        message.kwargs[param_name] = parse_obj_as(annot, value)
    except (ValueError, RuntimeError) as exc:
        logger.warning(
            "Can't parse argument %s for task %s. Reason: %s",
            param_name,
            message.task_name,
            exc,
            exc_info=True,
        )


def parse_params(
    signature: inspect.Signature | None,
    type_hints: dict[str, Any],
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
    for param_name, param in signature.parameters.items():
        # If parameter doesn't have an annotation.
        annot = type_hints.get(param_name)
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            # Every positional-capable parameter occupies a slot in
            # message.args, even if it has no type annotation.
            argnum += 1
            if annot is None:
                continue
            if argnum < len(message.args):
                # This parameter was passed positionally.
                _parse_arg(param_name, annot, argnum, message)
            else:
                # The parameter was passed as a kwarg or not at all.
                _parse_kwarg(param_name, annot, message)
        elif param.kind == inspect.Parameter.VAR_POSITIONAL:
            # All remaining positional arguments belong to *args.
            if annot is None:
                continue
            for i in range(argnum + 1, len(message.args)):
                _parse_arg(param_name, annot, i, message)
        else:
            # KEYWORD_ONLY and VAR_KEYWORD parameters are matched by name.
            if annot is None or param.kind == inspect.Parameter.VAR_KEYWORD:
                continue
            _parse_kwarg(param_name, annot, message)
