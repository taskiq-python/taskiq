from typing import Protocol, runtime_checkable


@runtime_checkable
class ReceiverObserver(Protocol):
    """
    Observer for receiver stats.

    This class is used to observe/collect metrics for the receiver.
    This includes semaphore usage, tasks in queue, etc.

    metrics tracked:
    - Number of tasks in queue
    - Number of tasks in execution (semaphore usage)
    """

    def on_prefetch_queue_size(self, size: int) -> None:
        """Called when the prefetch queue size changes."""
        ...

    def on_semaphore_status(self, available: int) -> None:
        """Called when semaphore availability changes."""
        ...

    def on_active_tasks_count(self, count: int) -> None:
        """Called when the number of active tasks changes."""
        ...

    def on_task_not_found(self, task_name: str) -> None:
        """Called when a received task is not registered."""
        ...

    def on_deserialize_error(self, raw: bytes, error: Exception) -> None:
        """Called when a message fails to deserialize."""
        ...
