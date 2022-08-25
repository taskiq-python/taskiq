import inspect
from collections import OrderedDict
from typing import Any, Callable, Coroutine, Optional, TypeVar

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend, TaskiqResult
from taskiq.cli.args import TaskiqArgs
from taskiq.cli.receiver import Receiver
from taskiq.exceptions import TaskiqError
from taskiq.message import BrokerMessage

_ReturnType = TypeVar("_ReturnType")


class InmemoryResultBackend(AsyncResultBackend[_ReturnType]):
    """
    Inmemory result backend.

    This resultbackend is intended to be used only
    with inmemory broker.

    It stores all results in a dict in memory.
    """

    def __init__(self, max_stored_results: int = 100) -> None:
        self.max_stored_results = max_stored_results
        self.results: OrderedDict[str, TaskiqResult[_ReturnType]] = OrderedDict()

    async def set_result(self, task_id: str, result: TaskiqResult[_ReturnType]) -> None:
        """
        Sets result.

        This method is used to store result of an execution in a
        results dict. But also it removes previous results
        to keep memory footprint as low as possible.

        :param task_id: id of a task.
        :param result: result of an execution.
        """
        if self.max_stored_results != -1:
            if len(self.results) >= self.max_stored_results:
                self.results.popitem(last=False)
        self.results[task_id] = result

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Checks wether result is ready.

        Readiness means that result with this task_id is
        present in results dict.

        :param task_id: id of a task to check.
        :return: True if ready.
        """
        return task_id in self.results

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Get result of a task.

        This method is used to get result
        from result dict.

        It throws exception in case if
        result dict doesn't have a value
        for task_id.

        :param task_id: id of a task.
        :param with_logs: this option is ignored.
        :return: result of a task execution.
        """
        return self.results[task_id]


class InMemoryBroker(AsyncBroker):
    """
    This broker is used to execute tasks without sending them elsewhere.

    It's useful for local development, if you don't want to setup real broker.
    """

    def __init__(  # noqa: WPS211
        self,
        sync_tasks_pool_size: int = 4,
        logs_format: Optional[str] = None,
        max_stored_results: int = 100,
        cast_types: bool = True,
        result_backend: Optional[AsyncResultBackend[Any]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
    ) -> None:
        if result_backend is None:
            result_backend = InmemoryResultBackend(
                max_stored_results=max_stored_results,
            )
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self.receiver = Receiver(
            self,
            TaskiqArgs(
                broker="",
                modules=[],
                max_threadpool_threads=sync_tasks_pool_size,
                no_parse=not cast_types,
                log_collector_format=logs_format or TaskiqArgs.log_collector_format,
            ),
        )

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking task.

        This method just executes given task.

        :param message: incomming message.

        :raises TaskiqError: if someone wants to kick unknown task.
        """
        target_task = self.available_tasks.get(message.task_name)
        if target_task is None:
            raise TaskiqError("Unknown task.")
        if self.receiver.task_signatures:
            if not self.receiver.task_signatures.get(target_task.task_name):
                self.receiver.task_signatures[
                    target_task.task_name
                ] = inspect.signature(
                    target_task.original_func,
                )

        await self.receiver.callback(message=message)

    async def listen(
        self,
        callback: Callable[[BrokerMessage], Coroutine[Any, Any, None]],
    ) -> None:
        """
        Inmemory broker cannot listen.

        This method throws RuntimeError if you call it.
        Because inmemory broker cannot really listen to any of tasks.

        :param callback: message callback.
        :raises RuntimeError: if this method is called.
        """
        raise RuntimeError("Inmemory brokers cannot listen.")
