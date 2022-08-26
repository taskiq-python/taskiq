import asyncio
import inspect
import io
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from time import time
from typing import Any, Callable, Dict, Optional

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.cli.args import TaskiqArgs
from taskiq.cli.log_collector import log_collector
from taskiq.cli.params_parser import parse_params
from taskiq.context import Context
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.result import TaskiqResult
from taskiq.utils import maybe_awaitable

logger = getLogger(__name__)


def inject_context(
    signature: Optional[inspect.Signature],
    message: TaskiqMessage,
    broker: AsyncBroker,
) -> None:
    """
    Inject context parameter in message's kwargs.

    This function parses signature to get
    the context parameter definition.

    If at least one parameter has the Context
    type, it will add current context as kwarg.

    :param signature: function's signature.
    :param message: current taskiq message.
    :param broker: current broker.
    """
    if signature is None:
        return
    for param_name, param in signature.parameters.items():
        if param.annotation is param.empty:
            continue
        if param.annotation is Context:
            message.kwargs[param_name] = Context(message.copy(), broker)


def _run_sync(target: Callable[..., Any], message: TaskiqMessage) -> Any:
    """
    Runs function synchronously.

    We use this function, because
    we cannot pass kwargs in loop.run_with_executor().

    :param target: function to execute.
    :param message: received message from broker.
    :return: result of function's execution.
    """
    return target(*message.args, **message.kwargs)


class Receiver:
    """Class that uses as a callback handler."""

    def __init__(self, broker: AsyncBroker, cli_args: TaskiqArgs) -> None:
        self.broker = broker
        self.cli_args = cli_args
        self.task_signatures: Dict[str, inspect.Signature] = {}
        for task in self.broker.available_tasks.values():
            self.task_signatures[task.task_name] = inspect.signature(task.original_func)
        self.executor = ThreadPoolExecutor(
            max_workers=cli_args.max_threadpool_threads,
        )

    async def callback(  # noqa: C901
        self,
        message: BrokerMessage,
        raise_err: bool = False,
    ) -> None:
        """
        Receive new message and execute tasks.

        This method is used to process message,
        that came from brokers.

        :raises Exception: if raise_err is true,
            and excpetion were found while saving result.
        :param message: received message.
        :param raise_err: raise an error if cannot save result in
            result_backend.
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
        result = await self.run_task(
            target=self.broker.available_tasks[message.task_name].original_func,
            message=taskiq_msg,
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
            if raise_err:
                raise exc

    async def run_task(  # noqa: C901, WPS210
        self,
        target: Callable[..., Any],
        message: TaskiqMessage,
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
        :return: result of execution.
        """
        loop = asyncio.get_running_loop()
        # Buffer to capture logs.
        logs = io.StringIO()
        returned = None
        found_exception = None
        signature = self.task_signatures.get(message.task_name)
        if self.cli_args.no_parse:
            signature = None
        parse_params(signature, message)
        inject_context(
            self.task_signatures.get(message.task_name),
            message,
            self.broker,
        )
        # Captures function's logs.
        with log_collector(logs, self.cli_args.log_collector_format):
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
                        self.executor,
                        _run_sync,
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
            for middleware in self.broker.middlewares:
                if middleware.__class__.on_error != TaskiqMiddleware.on_error:
                    await maybe_awaitable(
                        middleware.on_error(
                            message,
                            result,
                            found_exception,
                        ),
                    )

        return result