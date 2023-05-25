import asyncio
import inspect
from concurrent.futures import Executor
from logging import getLogger
from time import time
from typing import Any, Callable, Dict, Optional, Set, Union, get_type_hints

import anyio
from taskiq_dependencies import DependencyGraph

from taskiq.abc.broker import AckableMessage, AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.context import Context
from taskiq.exceptions import NoResultError
from taskiq.message import TaskiqMessage
from taskiq.receiver.params_parser import parse_params
from taskiq.result import TaskiqResult
from taskiq.state import TaskiqState
from taskiq.utils import maybe_awaitable

logger = getLogger(__name__)
QUEUE_DONE = b"-1"


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

    def __init__(  # noqa: WPS211
        self,
        broker: AsyncBroker,
        executor: Optional[Executor] = None,
        validate_params: bool = True,
        max_async_tasks: "Optional[int]" = None,
        max_prefetch: int = 0,
        propagate_exceptions: bool = True,
    ) -> None:
        self.broker = broker
        self.executor = executor
        self.validate_params = validate_params
        self.task_signatures: Dict[str, inspect.Signature] = {}
        self.task_hints: Dict[str, Dict[str, Any]] = {}
        self.dependency_graphs: Dict[str, DependencyGraph] = {}
        self.propagate_exceptions = propagate_exceptions
        for task in self.broker.available_tasks.values():
            self.task_signatures[task.task_name] = inspect.signature(task.original_func)
            self.task_hints[task.task_name] = get_type_hints(task.original_func)
            self.dependency_graphs[task.task_name] = DependencyGraph(task.original_func)
        self.sem: "Optional[asyncio.Semaphore]" = None
        if max_async_tasks is not None and max_async_tasks > 0:
            self.sem = asyncio.Semaphore(max_async_tasks)
        else:
            logger.warning(
                "Setting unlimited number of async tasks "
                + "can result in undefined behavior",
            )
        self.sem_prefetch = asyncio.Semaphore(max_prefetch)

    async def callback(  # noqa: C901, WPS213, WPS217
        self,
        message: Union[bytes, AckableMessage],
        raise_err: bool = False,
    ) -> None:
        """
        Receive new message and execute tasks.

        This method is used to process message,
        that came from brokers.

        :raises Exception: if raise_err is true,
            and exception were found while saving result.
        :param message: received message.
        :param raise_err: raise an error if cannot save result in
            result_backend.
        """
        if isinstance(message, AckableMessage):
            message_data = message.data
        else:
            message_data = message
        try:
            taskiq_msg = self.broker.formatter.loads(message=message_data)
        except Exception as exc:
            logger.warning(
                "Cannot parse message: %s. Skipping execution.\n %s",
                message_data,
                exc,
                exc_info=True,
            )
            return
        logger.debug(f"Received message: {taskiq_msg}")
        if taskiq_msg.task_name not in self.broker.available_tasks:
            logger.warning(
                'task "%s" is not found. Maybe you forgot to import it?',
                taskiq_msg.task_name,
            )
            return
        logger.debug(
            "Function for task %s is resolved. Executing...",
            taskiq_msg.task_name,
        )
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
            target=self.broker.available_tasks[taskiq_msg.task_name].original_func,
            message=taskiq_msg,
        )
        if isinstance(message, AckableMessage):
            await maybe_awaitable(message.ack())
        for middleware in self.broker.middlewares:
            if middleware.__class__.post_execute != TaskiqMiddleware.post_execute:
                await maybe_awaitable(middleware.post_execute(taskiq_msg, result))
        try:
            if not isinstance(result.error, NoResultError):
                await self.broker.result_backend.set_result(taskiq_msg.task_id, result)
                for middleware in self.broker.middlewares:
                    if middleware.__class__.post_save != TaskiqMiddleware.post_save:
                        await maybe_awaitable(middleware.post_save(taskiq_msg, result))
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
        returned = None
        found_exception = None
        signature = None
        if self.validate_params:
            signature = self.task_signatures.get(message.task_name)
        dependency_graph = self.dependency_graphs.get(message.task_name)
        parse_params(signature, self.task_hints.get(message.task_name) or {}, message)

        dep_ctx = None
        if dependency_graph:
            # Create a context for dependency resolving.
            broker_ctx = self.broker.custom_dependency_context
            broker_ctx.update(
                {
                    Context: Context(message, self.broker),
                    TaskiqState: self.broker.state,
                },
            )
            dep_ctx = dependency_graph.async_ctx(broker_ctx)
            # Resolve all function's dependencies.
            dep_kwargs = await dep_ctx.resolve_kwargs()
            for key, val in dep_kwargs.items():
                if key not in message.kwargs:
                    message.kwargs[key] = val
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
        except BaseException as exc:  # noqa: WPS424
            found_exception = exc
            logger.error(
                "Exception found while executing function: %s",
                exc,
                exc_info=True,
            )
        # Stop the timer.
        execution_time = time() - start_time
        if dep_ctx:
            args = (None, None, None)
            if found_exception and self.propagate_exceptions:
                args = (  # type: ignore
                    type(found_exception),
                    found_exception,
                    found_exception.__traceback__,
                )
            await dep_ctx.close(*args)

        # Assemble result.
        result: "TaskiqResult[Any]" = TaskiqResult(
            is_err=found_exception is not None,
            log=None,
            return_value=returned,
            execution_time=round(execution_time, 2),
            error=found_exception,
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

    async def listen(self) -> None:  # pragma: no cover
        """
        This function iterates over tasks asynchronously.

        It uses listen() method of an AsyncBroker
        to get new messages from queues.
        """
        await self.broker.startup()
        logger.info("Listening started.")
        queue: "asyncio.Queue[Union[bytes, AckableMessage]]" = asyncio.Queue()

        async with anyio.create_task_group() as gr:
            gr.start_soon(self.prefetcher, queue)
            gr.start_soon(self.runner, queue)

    async def prefetcher(
        self,
        queue: "asyncio.Queue[Union[bytes, AckableMessage]]",
    ) -> None:
        """
        Prefetch tasks data.

        :param queue: queue for prefetched data.
        """
        iterator = self.broker.listen()

        while True:
            try:
                await self.sem_prefetch.acquire()
                message = await iterator.__anext__()  # noqa: WPS609
                await queue.put(message)

            except StopAsyncIteration:
                break

        await queue.put(QUEUE_DONE)

    async def runner(
        self,
        queue: "asyncio.Queue[Union[bytes, AckableMessage]]",
    ) -> None:
        """
        Run tasks.

        :param queue: queue with prefetched data.
        """
        tasks: Set[asyncio.Task[Any]] = set()

        def task_cb(task: "asyncio.Task[Any]") -> None:
            """
            Callback for tasks.

            This function used to remove task
            from the list of active tasks and release
            the semaphore, so other tasks can use it.

            :param task: finished task
            """
            tasks.discard(task)
            if self.sem is not None:
                self.sem.release()

        while True:
            # Waits for semaphore to be released.
            if self.sem is not None:
                await self.sem.acquire()

            self.sem_prefetch.release()
            message = await queue.get()
            if message is QUEUE_DONE:
                break

            task = asyncio.create_task(
                self.callback(message=message, raise_err=False),
            )
            tasks.add(task)

            # We want the task to remove itself from the set when it's done.
            #
            # Because python's GC can silently cancel task
            # and it considered to be Hisenbug.
            # https://textual.textualize.io/blog/2023/02/11/the-heisenbug-lurking-in-your-async-code/
            task.add_done_callback(task_cb)
