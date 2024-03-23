import asyncio
import logging
from logging import LogRecord
from typing import Dict, List, Union

from bidict import bidict


class TaskiqLogHandler(logging.Handler):
    """Log handler class."""

    def __init__(self, level: Union[int, str] = 0) -> None:
        self.stream: Dict[Union[str, None], List[logging.LogRecord]] = {}
        self._associations: bidict[Union[str, None], Union[str, None]] = bidict()
        super().__init__(level)

    @staticmethod
    def _get_async_task_name() -> Union[str, None]:
        try:
            task = asyncio.current_task()
        except RuntimeError:
            return None
        else:
            if task:
                return task.get_name()

            raise RuntimeError

    def associate(self, task_id: str) -> None:
        """
        Associate the current async task with the Taskiq task ID.

        :param task_id: The Taskiq task ID.
        :type task_id: str
        """
        async_task_name = self._get_async_task_name()
        self._associations[task_id] = async_task_name

    def retrieve_logs(self, task_id: str) -> List[LogRecord]:
        """
        Collect logs.

        Collect the logs of a Taskiq task and return
        them after removing them from memory.

        :param task_id: The Taskiq task ID
        :type task_id: str
        :return: A list of LogRecords
        :rtype: List[LogRecord]
        """
        async_task_name = self._associations[task_id]
        try:
            stream = self.stream[async_task_name]
        except KeyError:
            stream = []
        else:
            del self._associations[task_id]
        return stream

    def emit(self, record: LogRecord) -> None:
        """
        Collect an outputted log record.

        :param record: The log record to collect.
        :type record: LogRecord
        """
        try:
            async_task_name = self._get_async_task_name()
        except RuntimeError:
            # If not in an async context, do nothing
            return
        record.async_task_name = async_task_name
        try:
            record.task_id = self._associations.inverse[async_task_name]
        except KeyError:
            return
        try:
            self.stream[async_task_name].append(record)
        except KeyError:
            self.stream[async_task_name] = [record]
