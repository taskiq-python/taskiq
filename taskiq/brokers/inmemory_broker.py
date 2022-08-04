import inspect
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import AsyncGenerator, Optional, TypeVar

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend, TaskiqResult
from taskiq.cli.async_task_runner import run_task
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

    def __init__(
        self,
        sync_tasks_pool_size: int = 4,
        logs_format: Optional[str] = None,
        max_stored_results: int = 100,
        cast_types: bool = True,
    ) -> None:
        super().__init__(
            InmemoryResultBackend(
                max_stored_results=max_stored_results,
            ),
        )
        self.executor = ThreadPoolExecutor(max_workers=sync_tasks_pool_size)
        self.cast_types = cast_types
        if logs_format is None:
            logs_format = "%(levelname)s %(message)s"
        self.logs_format = logs_format

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking task.

        This method just executes given task.

        :param message: incomming message.
        :raises TaskiqError: if someone wants to kick unknown task.
        """
        target_task = self.available_tasks.get(message.task_name)
        taskiq_message = self.formatter.loads(message=message)
        if target_task is None:
            raise TaskiqError("Unknown task.")
        result = await run_task(
            target=target_task.original_func,
            signature=inspect.signature(target_task.original_func),
            message=taskiq_message,
            log_collector_format=self.logs_format,
            executor=self.executor,
        )
        await self.result_backend.set_result(message.task_id, result)

    async def listen(self) -> AsyncGenerator[BrokerMessage, None]:  # type: ignore
        """
        Inmemory broker cannot listen.

        This method throws RuntimeError if you call it.
        Because inmemory broker cannot really listen to any of tasks.

        :raises RuntimeError: if this method is called.
        """
        raise RuntimeError("Inmemory brokers cannot listen.")
