import asyncio
import inspect
import io
from logging import getLogger
from time import time
from typing import Any, Callable, Dict, Optional

from pydantic import parse_obj_as

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import TaskiqResult
from taskiq.cli.args import TaskiqArgs
from taskiq.cli.log_collector import LogsCollector
from taskiq.message import TaskiqMessage

logger = getLogger("taskiq.worker")


def parse_params(  # noqa: C901
    signature: Optional[inspect.Signature],
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

    If you will kall my_task.kiq("11")

    You'll receive parsed 11 (int).
    But, if you call it with mytask.kiq("str"),
    you get the same value.

    If you want to skip parsing completely,
    you can pass --no-parse to worker,
    or you can make some of parameters untyped,
    or use Any.

    :param signature: original function's signature.
    :param message: incoming message.
    """
    if signature is None:
        return
    argnum = -1
    for param_name, params_type in signature.parameters.items():
        if params_type.annotation is params_type.empty:
            continue
        argnum += 1
        annot = params_type.annotation
        value = None
        logger.debug("Trying to parse %s as %s" % (param_name, params_type.annotation))
        if argnum >= len(message.args):
            value = message.kwargs.get(param_name)
            if value is None:
                continue
            try:
                message.kwargs[param_name] = parse_obj_as(annot, value)
            except (ValueError, RuntimeError) as exc:
                logger.debug(exc, exc_info=True)
        else:
            value = message.args[argnum]
            if value is None:
                continue
            try:
                message.args[argnum] = parse_obj_as(annot, value)
            except (ValueError, RuntimeError) as exc:  # noqa: WPS440
                logger.debug(exc, exc_info=True)


def run_sync(target: Callable[..., Any], message: TaskiqMessage) -> Any:
    """
    Runs function synchronously.

    We use this function, because
    we cannot pass kwargs in loop.run_with_executor().

    :param target: function to execute.
    :param message: received message from broker.
    :return: result of function's execution.
    """
    return target(*message.args, **message.kwargs)


async def run_task(  # noqa: WPS210
    target: Callable[..., Any],
    signature: Optional[inspect.Signature],
    message: TaskiqMessage,
    cli_args: TaskiqArgs,
) -> TaskiqResult[Any]:
    """
    This function actually executes functions.

    It has all needed parameters in
    message.

    If the target function is async
    it awaits it, if it's sync
    it wraps it in run_sync and executes in
    threadpool executor.

    Also it uses LogsCollector to
    collect logs.

    :param target: function to execute.
    :param signature: signature of an original function.
    :param message: received message.
    :param cli_args: CLI arguments for worker.
    :return: result of execution.
    """
    loop = asyncio.get_running_loop()
    logs = io.StringIO()
    is_err = False
    returned = None
    # Captures function's logs.
    parse_params(signature, message)
    with LogsCollector(logs, cli_args.log_collector_format):
        start_time = time()
        try:
            if asyncio.iscoroutinefunction(target):
                returned = await target(*message.args, **message.kwargs)
            else:
                returned = await loop.run_in_executor(
                    None,
                    run_sync,
                    target,
                    message,
                )
        except Exception as exc:
            is_err = True
            logger.error(
                "Exception found while executing function: %s" % exc,
                exc_info=True,
            )
        execution_time = time() - start_time

    raw_logs = logs.getvalue()
    logs.close()
    return TaskiqResult(
        is_err=is_err,
        log=raw_logs,
        return_value=returned,
        execution_time=execution_time,
    )


async def async_listen_messages(  # noqa: C901, WPS210
    broker: AsyncBroker,
    cli_args: TaskiqArgs,
) -> None:
    """
    This function iterates over tasks asynchronously.

    It uses listen() method of an AsyncBroker
    to get new messages from queues.

    :param broker: broker to listen to.
    :param cli_args: CLI arguments for worker.
    """
    logger.info("Runing startup event.")
    await broker.startup()
    logger.info("Listening started.")
    task_registry: Dict[str, Callable[..., Any]] = {}
    task_signatures: Dict[str, inspect.Signature] = {}
    for task in broker._related_tasks:  # noqa: WPS437
        task_registry[task.task_name] = task.original_func
        # If we need to parse parameters we remember all tasks signatures.
        if not cli_args.no_parse:
            task_signatures[task.task_name] = inspect.signature(task.original_func)
    async for message in broker.listen():
        logger.debug(f"Received message: {message}")
        if message.task_name not in task_registry:
            logger.warning(
                'task "%s" is not found. Maybe you forgot to import it?',
                message.task_name,
            )
            continue
        func = task_registry[message.task_name]
        logger.debug(
            "Function for task %s is resolved. Executing...",
            message.task_name,
        )
        result = await run_task(
            func,
            task_signatures.get(message.task_name),
            message,
            cli_args,
        )
        try:
            await broker.result_backend.set_result(message.task_id, result)
        except Exception as exc:
            logger.exception(exc)
