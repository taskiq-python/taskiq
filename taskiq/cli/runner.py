import asyncio
import io
from logging import getLogger
from typing import Any, Callable

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import TaskiqResult
from taskiq.cli.args import TaskiqArgs
from taskiq.cli.log_collector import LogsCollector
from taskiq.message import TaskiqMessage

logger = getLogger("taskiq.worker")


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


async def run_task(
    target: Callable[..., Any],
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
    :param message: received message.
    :param cli_args: CLI arguments for worker.
    :return: result of execution.
    """
    loop = asyncio.get_running_loop()
    logs = io.StringIO()
    is_err = False
    returned = None
    # Captures function's logs.
    with LogsCollector(logs, cli_args.log_collector_format):
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

    raw_logs = logs.getvalue()
    logs.close()
    return TaskiqResult(
        is_err=is_err,
        log=raw_logs,
        return_value=returned,
    )


async def async_listen_messages(broker: AsyncBroker, cli_args: TaskiqArgs) -> None:
    """
    This function iterates over tasks asynchronously.

    It uses listen() method of an AsyncBroker
    to get new messages from queues.

    :param broker: broker to listen to.
    :param cli_args: CLI arguments for worker.
    """
    logger.info("Listening started.")
    task_registry = {}
    for task in broker._related_tasks:  # noqa: WPS437
        task_registry[task.task_name] = task.original_func
    async for message in broker.listen():
        logger.debug(f"Received message: {message}")
        if message.task_name not in task_registry:
            logger.warning(
                "%s cannot be resolved. Maybe you forgot to import it?",
                message.task_name,
            )
            continue
        func = task_registry[message.task_name]
        logger.debug(
            "Function for task %s is resolved. Executing...",
            message.task_name,
        )
        result = await run_task(func, message, cli_args)
        try:
            await broker.result_backend.set_result(message.task_id, result)
        except Exception as exc:
            logger.exception(exc)
