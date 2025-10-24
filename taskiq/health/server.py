"""HTTP health check server for Kubernetes probes."""
import logging
import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from aiohttp import web
else:
    try:
        from aiohttp import web
    except ImportError as exc:
        raise ImportError(
            "Cannot import aiohttp. "
            "Please install 'taskiq[health]' to use health checks.",
        ) from exc

from .heartbeat import WorkerHeartbeatArray

logger = logging.getLogger("taskiq.health")


class HealthCheckServer:
    """HTTP server providing health check endpoints for Kubernetes."""

    def __init__(
        self,
        port: int,
        heartbeat_array: WorkerHeartbeatArray,
        timeout: float = 30.0,
        host: str = "0.0.0.0",
    ) -> None:
        """Initialize health check server.

        Args:
            port: HTTP server port
            heartbeat_array: Shared heartbeat array
            timeout: Seconds before worker considered dead
            host: HTTP server host (default: "0.0.0.0", use "::" for IPv6)
        """
        self.port = port
        self.host = host
        self.heartbeat_array = heartbeat_array
        self.timeout = timeout
        self.app: Optional[web.Application] = None
        self.runner: Optional[web.AppRunner] = None

    async def start(self) -> None:
        """Start HTTP health check server."""
        self.app = web.Application()
        self.app.router.add_get("/health/live", self.liveness_check)
        self.app.router.add_get("/health/ready", self.readiness_check)
        self.app.router.add_get("/health/status", self.detailed_status)

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()

        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()
        logger.info("Health check server started on %s:%d", self.host, self.port)

    async def stop(self) -> None:
        """Stop HTTP server."""
        if self.runner:
            await self.runner.cleanup()
            logger.info("Health check server stopped on port %d", self.port)

    async def liveness_check(self, request: "web.Request") -> "web.Response":
        """Kubernetes liveness probe - Strategy 3: Pass if any worker alive.

        Returns HTTP 200 if at least one worker is responsive,
        HTTP 503 if all workers are unresponsive.
        """
        alive_workers = self.heartbeat_array.get_alive_workers(self.timeout)

        if len(alive_workers) > 0:
            return web.json_response(
                {
                    "status": "healthy",
                    "alive_workers": (
                        f"{len(alive_workers)}/{self.heartbeat_array.num_workers}"
                    ),
                    "timestamp": time.time(),
                },
                headers={
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "X-Content-Type-Options": "nosniff",
                },
            )
        return web.json_response(
            {
                "status": "unhealthy",
                "reason": "All workers unresponsive",
                "alive_workers": f"0/{self.heartbeat_array.num_workers}",
                "timestamp": time.time(),
            },
            status=503,
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "X-Content-Type-Options": "nosniff",
            },
        )

    async def readiness_check(self, request: "web.Request") -> "web.Response":
        """Kubernetes readiness probe - same logic as liveness for now."""
        return await self.liveness_check(request)

    async def detailed_status(self, request: "web.Request") -> "web.Response":
        """Detailed worker status for debugging."""
        alive_workers = self.heartbeat_array.get_alive_workers(self.timeout)
        current_time = time.time()

        worker_details = []
        for worker_id in range(self.heartbeat_array.num_workers):
            last_heartbeat = self.heartbeat_array.heartbeats[worker_id]

            if last_heartbeat == 0:
                status = "not_started"
                seconds_ago = None
            else:
                seconds_ago = current_time - last_heartbeat
                status = "alive" if seconds_ago <= self.timeout else "dead"

            worker_details.append(
                {
                    "worker_id": worker_id,
                    "status": status,
                    "last_heartbeat": last_heartbeat,
                    "seconds_ago": seconds_ago,
                },
            )

        return web.json_response(
            {
                "overall_status": "healthy" if len(alive_workers) > 0 else "unhealthy",
                "alive_workers": len(alive_workers),
                "total_workers": self.heartbeat_array.num_workers,
                "timeout_seconds": self.timeout,
                "workers": worker_details,
                "timestamp": current_time,
            },
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "X-Content-Type-Options": "nosniff",
            },
        )
