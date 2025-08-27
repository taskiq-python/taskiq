"""Heartbeat array for worker health monitoring."""
import time
from multiprocessing import Array
from typing import Dict, List, Union


class WorkerHeartbeatArray:
    """Fast shared memory array for worker heartbeat tracking."""

    def __init__(self, num_workers: int) -> None:
        """Initialize heartbeat array.

        Args:
            num_workers: Number of worker processes to monitor
        """
        self.num_workers = num_workers
        # Array of doubles (timestamps), one slot per worker
        self.heartbeats = Array("d", [0.0] * num_workers)

    def update_worker_heartbeat(self, worker_id: int) -> None:
        """Update heartbeat timestamp for a specific worker.

        Called by worker process to report heartbeat.

        Args:
            worker_id: ID of the worker process (0-based index)
        """
        if 0 <= worker_id < self.num_workers:
            self.heartbeats[worker_id] = time.time()

    def get_alive_workers(
        self,
        timeout: float = 30.0,
    ) -> List[Dict[str, Union[int, float]]]:
        """Get list of alive workers based on heartbeat timeout.

        Called by main process to check worker health.

        Args:
            timeout: Seconds after which worker is considered dead

        Returns:
            List of dictionaries with worker status information
        """
        current_time = time.time()
        alive_workers = []

        for worker_id in range(self.num_workers):
            last_heartbeat = self.heartbeats[worker_id]
            if last_heartbeat > 0 and (current_time - last_heartbeat) <= timeout:
                alive_workers.append(
                    {
                        "worker_id": worker_id,
                        "last_heartbeat": last_heartbeat,
                        "seconds_ago": current_time - last_heartbeat,
                    },
                )

        return alive_workers

    def is_any_worker_alive(self, timeout: float = 30.0) -> bool:
        """Check if any worker is alive.

        Strategy 3: Pass if ANY worker is alive.

        Args:
            timeout: Seconds after which worker is considered dead

        Returns:
            True if at least one worker is responsive
        """
        return len(self.get_alive_workers(timeout)) > 0

    def get_worker_status(self, worker_id: int, timeout: float = 30.0) -> str:
        """Get status of a specific worker.

        Args:
            worker_id: ID of the worker to check
            timeout: Seconds after which worker is considered dead

        Returns:
            Status string: 'not_started', 'alive', or 'dead'
        """
        if worker_id < 0 or worker_id >= self.num_workers:
            return "invalid_id"

        last_heartbeat = self.heartbeats[worker_id]

        if last_heartbeat == 0:
            return "not_started"

        current_time = time.time()
        seconds_ago = current_time - last_heartbeat

        return "alive" if seconds_ago <= timeout else "dead"
