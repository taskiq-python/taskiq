from abc import ABC, abstractmethod

from taskiq import TaskiqScheduler


class TaskiqSchedulerFactory(ABC):
    """SchedulerFactory class."""

    @abstractmethod
    def get_scheduler(self) -> TaskiqScheduler:
        """
        Get scheduler instance.

        :return: TaskiqScheduler instance
        """
