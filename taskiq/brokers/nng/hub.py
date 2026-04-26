"""
NNG hub: central control plane, task dispatcher, and lease manager.

Run as a standalone process::

    taskiq-nng-hub --control-addr ipc:///tmp/taskiq-nng.ipc

Or embed it in an application for testing::

    hub = NNGHub(HubConfig(control_addr="ipc:///tmp/h.ipc"))
    await hub.start()
    ...
    await hub.stop()
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import logging
import os
import signal
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

try:
    import pynng  # type: ignore
except ImportError:
    pynng = None  # type: ignore[assignment]

from .protocol import (
    ControlMessage,
    ControlResponse,
    TaskEnvelope,
    WorkerState,
)
from .storage import (
    InMemoryStore,
    QueueFullError,
    RoutingPolicy,
    StoreConfig,
    make_routing_policy,
)

logger = logging.getLogger(__name__)


@dataclass
class HubConfig:
    """Configuration for :class:`NNGHub`."""

    control_addr: str
    task_db: str = ""  # kept for API compat; ignored by in-memory store
    max_pending: int = 10_000
    heartbeat_timeout: float = 15.0
    lease_timeout: float = 20.0
    dispatch_interval: float = 0.05
    reaper_interval: float = 0.5
    routing_policy: RoutingPolicy | str = "least_loaded"
    backoff_cap: float = 60.0
    # Number of concurrent Rep0 contexts.  Each context handles one req/rep
    # pair independently; N contexts ≈ N simultaneous control-plane clients.
    control_concurrency: int = 16
    dispatch_batch: int = 50
    # Per-context recv timeout in ms.  Allows the stop event to be checked
    # even when there are no incoming messages.
    recv_timeout_ms: int = 1_000


class NNGHub:
    """
    Stateful central hub: control plane, task dispatcher, and lease manager.

    Architecture
    ────────────
    **Control plane** — ``Rep0`` socket with ``control_concurrency``
    independent ``nng_ctx`` contexts running concurrently.  Each context
    handles one request-reply at a time, so N workers can
    register/heartbeat/ack simultaneously without queuing behind each other.

    **Data plane** — One ``Push0`` socket per registered worker, dialed to
    the worker's own ``Pull0`` listen address.  The hub explicitly targets
    the least-loaded worker instead of relying on NNG round-robin.

    **State** — :class:`~taskiq.brokers.nng.storage.InMemoryStore`.  All
    store operations are synchronous and execute directly on the asyncio event
    loop without blocking (no I/O, no syscalls).

    **Recovery** — On startup, any tasks that were leased before the hub last
    stopped (within the same process lifetime) are automatically requeued by
    :meth:`~InMemoryStore.recover_dead_workers`.
    """

    def __init__(self, config: HubConfig) -> None:
        """
        Initialise the hub with the given configuration.

        :param config: hub configuration.
        """
        if pynng is None:
            raise RuntimeError(
                "pynng is required to use NNGHub.  "
                "Install it with: pip install taskiq[nng]"
            )
        self.config = config
        self.store = InMemoryStore(
            StoreConfig(
                max_pending=config.max_pending,
                lease_timeout=config.lease_timeout,
                backoff_cap=config.backoff_cap,
            ),
        )
        # Resolve once at construction so RoundRobin and similar stateful
        # policies maintain their counter across dispatch calls.
        self._routing: RoutingPolicy = make_routing_policy(config.routing_policy)
        self._stop = asyncio.Event()
        self._ctrl_sock: Any = None  # pynng.Rep0
        self._worker_push: dict[str, Any] = {}  # worker_id -> pynng.Push0
        self._tasks: list[asyncio.Task[None]] = []

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the hub: recover orphaned tasks, open sockets, spawn loops."""
        self.store.recover_dead_workers(self.config.heartbeat_timeout)

        self._ctrl_sock = pynng.Rep0(listen=self.config.control_addr)
        self._ctrl_sock.recv_timeout = self.config.recv_timeout_ms

        self._tasks = [
            asyncio.create_task(self._dispatch_loop(), name="hub-dispatch"),
            asyncio.create_task(self._reaper_loop(), name="hub-reaper"),
        ]
        for i in range(self.config.control_concurrency):
            ctx = self._ctrl_sock.new_context()
            self._tasks.append(
                asyncio.create_task(
                    self._control_handler(ctx), name=f"hub-ctrl-{i}"
                ),
            )
        logger.info("NNG hub started on %s", self.config.control_addr)

    async def stop(self) -> None:
        """Gracefully stop all hub loops and close sockets."""
        logger.info("NNG hub stopping…")
        self._stop.set()
        for t in self._tasks:
            t.cancel()
            with suppress(asyncio.CancelledError):
                await t
        for sock in self._worker_push.values():
            with suppress(Exception):
                sock.close()
        self._worker_push.clear()
        if self._ctrl_sock is not None:
            with suppress(Exception):
                self._ctrl_sock.close()
        logger.info("NNG hub stopped")

    # ── control plane ─────────────────────────────────────────────────────────

    async def _control_handler(self, ctx: Any) -> None:
        """Run one Rep0 context: receive → dispatch → reply, in a loop."""
        while not self._stop.is_set():
            try:
                raw = await ctx.arecv()
            except pynng.Timeout:
                continue
            except (pynng.Closed, asyncio.CancelledError):
                break
            except Exception as exc:
                logger.warning("Control recv error: %s", exc)
                continue

            try:
                response = await self._handle(raw)
            except Exception as exc:
                logger.exception("Unhandled error in control handler")
                response = ControlResponse(ok=False, error=str(exc))

            try:
                await ctx.asend(response.to_bytes())
            except (pynng.Closed, asyncio.CancelledError):
                break
            except Exception as exc:
                logger.warning("Control send error: %s", exc)

    async def _handle(self, raw: bytes) -> ControlResponse:  # noqa: PLR0911, C901
        """Dispatch a raw control message to the appropriate handler."""
        msg = ControlMessage.from_bytes(raw)

        if msg.kind == "ping":
            return ControlResponse(ok=True, payload={"pong": True})

        if msg.kind == "submit":
            return await self._handle_submit(msg.payload)

        if msg.kind == "register":
            return await self._handle_register(msg.payload)

        if msg.kind == "heartbeat":
            self.store.heartbeat(msg.payload["worker_id"])
            return ControlResponse(ok=True, payload={"ok": True})

        if msg.kind == "unregister":
            return await self._handle_unregister(msg.payload["worker_id"])

        if msg.kind == "drain":
            self.store.mark_draining(msg.payload["worker_id"])
            return ControlResponse(ok=True, payload={"draining": True})

        if msg.kind == "ack":
            ok = self.store.ack(
                msg.payload["task_id"],
                msg.payload["worker_id"],
                msg.payload["lease_id"],
            )
            return ControlResponse(ok=ok, payload={"acked": ok})

        if msg.kind == "nack":
            ok = self.store.nack(
                msg.payload["task_id"],
                msg.payload["worker_id"],
                msg.payload["lease_id"],
                msg.payload.get("error", "unknown error"),
            )
            return ControlResponse(ok=ok, payload={"nacked": ok})

        if msg.kind == "status":
            task = self.store.get_task(msg.payload["task_id"])
            return ControlResponse(ok=bool(task), payload=task or {})

        if msg.kind == "stats":
            return ControlResponse(ok=True, payload=self.store.stats())

        return ControlResponse(ok=False, error=f"unknown kind: {msg.kind!r}")

    async def _handle_submit(self, payload: dict[str, Any]) -> ControlResponse:
        envelope = TaskEnvelope(**payload)
        try:
            self.store.submit(envelope)
            return ControlResponse(ok=True, payload={"task_id": envelope.task_id})
        except QueueFullError:
            return ControlResponse(ok=False, error="queue full")

    async def _handle_register(self, payload: dict[str, Any]) -> ControlResponse:
        worker = WorkerState(**payload)
        self.store.register_worker(worker)
        if worker.worker_id not in self._worker_push:
            try:
                sock = pynng.Push0(dial=worker.task_addr)
                self._worker_push[worker.worker_id] = sock
            except Exception as exc:
                logger.error(
                    "Failed to dial worker %s at %s: %s",
                    worker.worker_id, worker.task_addr, exc,
                )
                return ControlResponse(ok=False, error=f"dial failed: {exc}")
        return ControlResponse(ok=True, payload={"registered": True})

    async def _handle_unregister(self, worker_id: str) -> ControlResponse:
        self.store.unregister_worker(worker_id)
        sock = self._worker_push.pop(worker_id, None)
        if sock is not None:
            with suppress(Exception):
                sock.close()
        return ControlResponse(ok=True, payload={"unregistered": True})

    # ── dispatch loop ─────────────────────────────────────────────────────────

    async def _dispatch_loop(self) -> None:
        while not self._stop.is_set():
            try:
                sent = await self._dispatch_once()
                if not sent:
                    await asyncio.sleep(self.config.dispatch_interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Dispatch loop error")
                await asyncio.sleep(self.config.dispatch_interval)

    async def _dispatch_once(self) -> bool:
        """Dispatch up to ``dispatch_batch`` due tasks to available workers."""
        due = self.store.due_tasks(self.config.dispatch_batch)
        if not due:
            return False
        sent_any = False
        for row in due:
            worker = self.store.choose_worker(
                self._routing,
                heartbeat_timeout=self.config.heartbeat_timeout,
            )
            if worker is None:
                return sent_any  # no capacity; leave remaining tasks in queue

            worker_id = worker["worker_id"]
            lease_id = uuid.uuid4().hex
            lease_until = time.time() + self.config.lease_timeout

            if not self.store.mark_leased(
                row["task_id"], worker_id, lease_id, lease_until,
            ):
                continue  # concurrent dispatch race; task already taken

            sock = self._worker_push.get(worker_id)
            if sock is None:
                logger.warning(
                    "No push socket for worker %s, requeueing %s",
                    worker_id, row["task_id"],
                )
                self.store.nack(row["task_id"], worker_id, lease_id, "no socket")
                continue

            envelope = TaskEnvelope(
                task_id=row["task_id"],
                task_name=row["task_name"],
                payload_b64=base64.b64encode(row["payload"]).decode("ascii"),
                labels=row["labels"],
                lease_id=lease_id,
                attempts=int(row["attempts"]) + 1,
                max_retries=int(row["max_retries"]),
                retry_backoff=float(row["retry_backoff"]),
                retry_jitter=float(row["retry_jitter"]),
                priority=int(row["priority"]),
                created_at=float(row["created_at"]),
            )
            try:
                await sock.asend(envelope.to_bytes())
                sent_any = True
            except Exception as exc:
                logger.warning(
                    "Failed to deliver %s to worker %s: %s",
                    row["task_id"], worker_id, exc,
                )
                self.store.nack(
                    row["task_id"], worker_id, lease_id,
                    f"dispatch send failed: {exc}",
                )
        return sent_any

    # ── reaper loop ───────────────────────────────────────────────────────────

    async def _reaper_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self.config.reaper_interval)
                reaped = self.store.reap_expired_leases()
                if reaped:
                    logger.debug("Reaped %d expired leases", reaped)
                recovered = self.store.recover_dead_workers(
                    self.config.heartbeat_timeout,
                )
                if recovered:
                    logger.info("Requeued %d tasks from dead workers", recovered)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Reaper loop error")


# ── standalone CLI entry point ────────────────────────────────────────────────

def _build_config() -> HubConfig:
    p = argparse.ArgumentParser(
        description="taskiq-nng-hub — NNG task router, dispatcher, and lease manager",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--control-addr",
        default=os.getenv("NNG_CONTROL_ADDR", "ipc:///tmp/taskiq-nng.ipc"),
        help="NNG address the hub listens on.  Env: NNG_CONTROL_ADDR",
    )
    p.add_argument(
        "--max-pending",
        type=int,
        default=int(os.getenv("NNG_MAX_PENDING", "10000")),
    )
    p.add_argument(
        "--heartbeat-timeout",
        type=float,
        default=float(os.getenv("NNG_HEARTBEAT_TIMEOUT", "15.0")),
        help="Seconds of silence before a worker is declared dead.",
    )
    p.add_argument(
        "--lease-timeout",
        type=float,
        default=float(os.getenv("NNG_LEASE_TIMEOUT", "20.0")),
        help="Seconds before an unacked task lease is reaped.",
    )
    p.add_argument(
        "--routing-policy",
        choices=["least_loaded", "p2c", "round_robin"],
        default=os.getenv("NNG_ROUTING_POLICY", "least_loaded"),
    )
    p.add_argument(
        "--control-concurrency",
        type=int,
        default=int(os.getenv("NNG_CONTROL_CONCURRENCY", "16")),
        help="Number of concurrent Rep0 contexts.",
    )
    p.add_argument(
        "--log-level",
        default=os.getenv("NNG_LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = p.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)-24s %(levelname)-8s %(message)s",
    )
    return HubConfig(
        control_addr=args.control_addr,
        max_pending=args.max_pending,
        heartbeat_timeout=args.heartbeat_timeout,
        lease_timeout=args.lease_timeout,
        routing_policy=args.routing_policy,
        control_concurrency=args.control_concurrency,
    )


async def _run(config: HubConfig) -> None:
    hub = NNGHub(config)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _on_signal() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_signal)

    await hub.start()
    try:
        await stop_event.wait()
    finally:
        await hub.stop()


def main() -> None:
    """Entry point for the ``taskiq-nng-hub`` CLI command."""
    config = _build_config()
    try:
        asyncio.run(_run(config))
    except KeyboardInterrupt:
        pass
