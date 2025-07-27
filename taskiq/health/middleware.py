"""Heartbeat middleware for worker health reporting."""
import asyncio
import logging
from contextlib import suppress
from typing import Any, Optional

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

from .heartbeat import WorkerHeartbeatArray

logger = logging.getLogger("taskiq.health")


class HeartbeatMiddleware(TaskiqMiddleware):
    """Middleware that reports worker heartbeats to shared memory array."""

    def __init__(
        self,
        heartbeat_array: WorkerHeartbeatArray,
        worker_id: int,
        heartbeat_interval: float = 10.0,
    ) -> None:
        """Initialize heartbeat middleware.

        Args:
            heartbeat_array: Shared memory array for heartbeat storage
            worker_id: Unique ID of this worker process
            heartbeat_interval: Seconds between heartbeat updates
        """
        super().__init__()
        self.heartbeat_array = heartbeat_array
        self.worker_id = worker_id
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_task: Optional[asyncio.Task[None]] = None
        self._stopped = False

    async def startup(self) -> None:
        """Start heartbeat reporting only in worker processes."""
        if self.broker.is_worker_process:
            logger.debug("Starting heartbeat for worker %d", self.worker_id)
            # Send initial heartbeat
            self.heartbeat_array.update_worker_heartbeat(self.worker_id)
            # Start periodic heartbeat task
            self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def shutdown(self) -> None:
        """Stop heartbeat reporting."""
        self._stopped = True
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.heartbeat_task
            logger.debug("Stopped heartbeat for worker %d", self.worker_id)

    async def _heartbeat_loop(self) -> None:
        """Report heartbeat every interval seconds."""
        while not self._stopped:
            try:
                self.heartbeat_array.update_worker_heartbeat(self.worker_id)
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(
                    "Heartbeat error for worker %d: %s",
                    self.worker_id,
                    exc,
                    exc_info=True,
                )
                # Continue heartbeat loop even on errors
                await asyncio.sleep(1.0)

    def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        """Update heartbeat before task execution."""
        if self.broker.is_worker_process:
            self.heartbeat_array.update_worker_heartbeat(self.worker_id)
        return message

    def post_execute(self, message: TaskiqMessage, result: TaskiqResult[Any]) -> None:
        """Update heartbeat after task execution."""
        if self.broker.is_worker_process:
            self.heartbeat_array.update_worker_heartbeat(self.worker_id)
