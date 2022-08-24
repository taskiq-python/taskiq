import asyncio
import inspect
import io
from concurrent.futures import Executor, ThreadPoolExecutor
from logging import getLogger
from time import time
from typing import Any, Callable, Dict, List, Optional

from pydantic import parse_obj_as

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.cli.args import TaskiqArgs
from taskiq.cli.log_collector import log_collector
from taskiq.context import Context, context_updater
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.result import TaskiqResult
from taskiq.utils import maybe_awaitable

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
    # Iterate over function's params.
    for param_name, params_type in signature.parameters.items():
        # If parameter doesn't have an annotation.
        if params_type.annotation is params_type.empty:
            continue
        # Increment argument numbers. This is
        # for positional arguments.
        argnum += 1
        # Shortland for params_type.annotation
        annot = params_type.annotation
        # Value from incoming message.
        value = None
        logger.debug("Trying to parse %s as %s", param_name, params_type.annotation)
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


async def run_task(  # noqa: C901, WPS210, WPS211
    target: Callable[..., Any],
    message: TaskiqMessage,
    signature: Optional[inspect.Signature] = None,
    log_collector_format: str = "%(message)s",
    executor: Optional[Executor] = None,
    middlewares: Optional[List[TaskiqMiddleware]] = None,
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
    :param log_collector_format: Log format in wich logs are collected.
    :param executor: executor to run sync tasks.
    :param middlewares: list of broker's middlewares in case of errors.
    :return: result of execution.
    """
    if middlewares is None:
        middlewares = []

    loop = asyncio.get_running_loop()
    # Buffer to capture logs.
    logs = io.StringIO()
    returned = None
    found_exception = None
    parse_params(signature, message)
    # Captures function's logs.
    with log_collector(logs, log_collector_format):
        # Start a timer.
        start_time = time()
        try:
            # If the function is a coroutine we await it.
            if asyncio.iscoroutinefunction(target):
                returned = await target(*message.args, **message.kwargs)
            else:
                # If this is a synchronous function we
                # run it in executor.
                returned = await loop.run_in_executor(
                    executor,
                    run_sync,
                    target,
                    message,
                )
        except Exception as exc:
            found_exception = exc
            logger.error(
                "Exception found while executing function: %s",
                exc,
                exc_info=True,
            )
        # Stop the timer.
        execution_time = time() - start_time

    raw_logs = logs.getvalue()
    logs.close()
    # Assemble result.
    result: "TaskiqResult[Any]" = TaskiqResult(
        is_err=found_exception is not None,
        log=raw_logs,
        return_value=returned,
        execution_time=execution_time,
    )
    # If exception is found we execute middlewares.
    if found_exception is not None:
        for middleware in middlewares:
            if middleware.__class__.on_error != TaskiqMiddleware.on_error:
                await maybe_awaitable(
                    middleware.on_error(
                        message,
                        result,
                        found_exception,
                    ),
                )

    return result


class Receiver:
    """Class that uses as a callback handler."""

    def __init__(self, broker: AsyncBroker, cli_args: TaskiqArgs) -> None:
        self.broker = broker
        self.cli_args = cli_args
        self.task_signatures: Dict[str, inspect.Signature] = {}
        if not cli_args.no_parse:
            for task in self.broker.available_tasks.values():
                self.task_signatures[task.task_name] = inspect.signature(
                    task.original_func,
                )
        self.executor = ThreadPoolExecutor(
            max_workers=cli_args.max_threadpool_threads,
        )

    async def callback(self, message: BrokerMessage) -> None:  # noqa: C901
        """
        Receive new message and execute tasks.

        This method is used to process message,
        that came from brokers.

        :param message: received message.
        """
        logger.debug(f"Received message: {message}")
        if message.task_name not in self.broker.available_tasks:
            logger.warning(
                'task "%s" is not found. Maybe you forgot to import it?',
                message.task_name,
            )
            return
        logger.debug(
            "Function for task %s is resolved. Executing...",
            message.task_name,
        )
        try:
            taskiq_msg = self.broker.formatter.loads(message=message)
        except Exception as exc:
            logger.warning(
                "Cannot parse message: %s. Skipping execution.\n %s",
                message,
                exc,
                exc_info=True,
            )
            return
        for middleware in self.broker.middlewares:
            if middleware.__class__.pre_execute != TaskiqMiddleware.pre_execute:
                taskiq_msg = await maybe_awaitable(
                    middleware.pre_execute(
                        taskiq_msg,
                    ),
                )

        logger.info(
            "Executing task %s with ID: %s",
            taskiq_msg.task_name,
            taskiq_msg.task_id,
        )
        with context_updater(Context(taskiq_msg, self.broker)):
            result = await run_task(
                target=self.broker.available_tasks[message.task_name].original_func,
                signature=self.task_signatures.get(message.task_name),
                message=taskiq_msg,
                log_collector_format=self.cli_args.log_collector_format,
                executor=self.executor,
                middlewares=self.broker.middlewares,
            )
        for middleware in self.broker.middlewares:
            if middleware.__class__.post_execute != TaskiqMiddleware.post_execute:
                await maybe_awaitable(middleware.post_execute(taskiq_msg, result))
        try:
            await self.broker.result_backend.set_result(message.task_id, result)
        except Exception as exc:
            logger.exception(
                "Can't set result in result backend. Cause: %s",
                exc,
                exc_info=True,
            )


async def async_listen_messages(
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
    logger.info("Inicialized receiver.")
    receiver = Receiver(broker, cli_args)
    logger.info("Listening started.")
    await broker.listen(receiver.callback)
