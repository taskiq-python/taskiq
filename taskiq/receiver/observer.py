from typing import Protocol, runtime_checkable


@runtime_checkable
class ReceiverObserver(Protocol):
    """
    Observer for reciever stats.

    This classs is used to observe/collect metrics for the receiver.
    This includes semaphore usage, tasks in queue, etc.

    metrics tracked:
    - Number of tasks in queue
    - Number of taks in execution (Semaphore uusage)
    """

    def on_prefetch_queue_size(self, size: int) -> None: ...
    def on_semaphore_status(self, available: int) -> None: ...
    def on_active_tasks_count(self, count: int) -> None: ...
    def on_task_not_found(self, task_name: str) -> None: ...
    def on_deserialize_error(self, raw: bytes, error: Exception) -> None: ...
