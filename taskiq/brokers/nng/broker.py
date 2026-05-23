"""NNG broker for taskiq — backed by a standalone :class:`NNGHub`."""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import tempfile
import time
import uuid
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress
from typing import Any, TypeVar

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.acks import AckableMessage
from taskiq.message import BrokerMessage

from .protocol import (
    ControlMessage,
    ControlResponse,
    TaskEnvelope,
    WorkerState,
    WorkerStatus,
)

try:
    import pynng  # type: ignore
except ImportError:
    pynng = None  # type: ignore[assignment]

_T = TypeVar("_T")

logger = logging.getLogger(__name__)


def _ipc_addr(prefix: str = "taskiq-nng") -> str:
    name = f"{prefix}-{os.getpid()}-{uuid.uuid4().hex[:8]}.ipc"
    return f"ipc://{os.path.join(tempfile.gettempdir(), name)}"


class NNGBroker(AsyncBroker):
    """
    Taskiq broker backed by a standalone :class:`~taskiq.brokers.nng_hub.NNGHub`.

    The hub must be running before workers or clients start.  Launch it with::

        taskiq-nng-hub --control-addr ipc:///tmp/taskiq-nng.ipc

    **Client mode** (``is_worker_process = False``)
        Only the control socket is opened.  :meth:`kick` submits tasks to the
        hub via a Req0 → Rep0 round-trip.

    **Worker mode** (``is_worker_process = True``)
        In addition to the control socket the broker opens a unique Pull0
        socket, registers with the hub, and runs a heartbeat loop.
        :meth:`listen` yields :class:`~taskiq.acks.AckableMessage` instances
        whose ``ack`` callback sends the correct ``lease_id`` back to the hub.

    Thread / coroutine safety
    ─────────────────────────
    ``Req0`` is strictly serial (one request in-flight per socket).
    ``_ctrl_lock`` serialises all :meth:`_send_control` calls so that
    concurrent coroutines (heartbeat + ack + kick) never interleave frames.

    Ack correctness
    ───────────────
    The hub embeds the dispatch-generated ``lease_id`` inside every
    :class:`~taskiq.brokers.nng_protocol.TaskEnvelope`.  The ack closure
    captures it directly, so validation on the hub side always succeeds for
    genuine acks and correctly rejects late/duplicate ones.
    """

    def __init__(
        self,
        control_addr: str,
        *,
        result_backend: "AsyncResultBackend[_T] | None" = None,
        task_id_generator: Callable[[], str] | None = None,
        worker_task_addr: str | None = None,
        worker_id: str | None = None,
        heartbeat_interval: float = 5.0,
        lease_timeout: float = 20.0,
        capacity: int = 1,
        recv_timeout_ms: int = 5_000,
        send_timeout_ms: int = 5_000,
    ) -> None:
        """
        Initialise the NNG broker.

        :param control_addr: NNG address of the hub's Rep0 control socket.
        :param result_backend: optional result backend.
        :param task_id_generator: optional task ID generator.
        :param worker_task_addr: NNG address this worker's Pull0 listens on.
            Defaults to a unique per-process IPC path.
        :param worker_id: stable identifier for this worker process.
            Defaults to ``<pid>-<uuid>``.
        :param heartbeat_interval: seconds between heartbeat messages to hub.
        :param lease_timeout: seconds a dispatched task lease remains valid.
        :param capacity: max concurrent tasks this worker will accept.
        :param recv_timeout_ms: Req0 recv timeout in milliseconds.
        :param send_timeout_ms: Req0 send timeout in milliseconds.
        """
        if pynng is None:
            raise RuntimeError(
                "pynng is required to use NNGBroker.  "
                "Install it with: pip install taskiq[nng]",
            )
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self.control_addr = control_addr
        self.worker_task_addr = worker_task_addr or _ipc_addr()
        self.worker_id = worker_id or f"{os.getpid()}-{uuid.uuid4().hex[:12]}"
        self.heartbeat_interval = heartbeat_interval
        self.lease_timeout = lease_timeout
        self.capacity = capacity
        self.recv_timeout_ms = recv_timeout_ms
        self.send_timeout_ms = send_timeout_ms

        self._ctrl_sock: Any = None   # pynng.Req0
        self._task_sock: Any = None   # pynng.Pull0 (worker mode only)
        self._heartbeat_task: asyncio.Task[None] | None = None
        # Req0 allows exactly one request in-flight; this lock enforces that.
        self._ctrl_lock = asyncio.Lock()

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def startup(self) -> None:
        """Open sockets, register with hub (worker mode), and start heartbeat."""
        self._ctrl_sock = pynng.Req0(
            dial=self.control_addr,
            recv_timeout=self.recv_timeout_ms,
            send_timeout=self.send_timeout_ms,
        )
        if self.is_worker_process:
            # recv_buffer_size lets the hub pre-queue up to `capacity` task
            # messages in NNG's recv buffer before listen() calls arecv().
            self._task_sock = pynng.Pull0(
                listen=self.worker_task_addr,
                recv_buffer_size=self.capacity,
            )
            resp = await self._send_control(
                "register",
                {
                    "worker_id": self.worker_id,
                    "task_addr": self.worker_task_addr,
                    "capacity": self.capacity,
                    "inflight": 0,
                    "last_seen": time.time(),
                    "heartbeat_interval": self.heartbeat_interval,
                    "lease_timeout": self.lease_timeout,
                    "draining": False,
                    "status": str(WorkerStatus.STARTING),
                    "version": "taskiq-nng",
                },
            )
            if not resp.ok:
                raise RuntimeError(f"Worker registration failed: {resp.error}")
            logger.info(
                "Worker %s registered with hub at %s",
                self.worker_id,
                self.control_addr,
            )
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(),
                name=f"nng-hb-{self.worker_id[:8]}",
            )
        await super().startup()

    async def shutdown(self) -> None:
        """Drain, unregister, cancel heartbeat, and close all sockets."""
        if self.is_worker_process:
            if self._heartbeat_task is not None:
                self._heartbeat_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._heartbeat_task
            if self._ctrl_sock is not None:
                with suppress(Exception):
                    await self._send_control(
                        "drain", {"worker_id": self.worker_id},
                    )
                    await self._send_control(
                        "unregister", {"worker_id": self.worker_id},
                    )
        if self._task_sock is not None:
            with suppress(Exception):
                self._task_sock.close()
        if self._ctrl_sock is not None:
            with suppress(Exception):
                self._ctrl_sock.close()
        await super().shutdown()

    # ── internal helpers ──────────────────────────────────────────────────────

    async def _send_control(
        self, kind: str, payload: dict[str, Any],
    ) -> ControlResponse:
        if self._ctrl_sock is None:
            raise RuntimeError("Control socket is not open (call startup() first)")
        async with self._ctrl_lock:
            await self._ctrl_sock.asend(
                ControlMessage(kind=kind, payload=payload).to_bytes(),
            )
            raw = await self._ctrl_sock.arecv()
        return ControlResponse.from_bytes(raw)

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                resp = await self._send_control(
                    "heartbeat", {"worker_id": self.worker_id},
                )
                if not resp.ok:
                    logger.warning("Heartbeat rejected by hub: %s", resp.error)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Hub may be temporarily unreachable; log and keep trying.
                logger.warning("Heartbeat failed: %s", exc)

    # ── AsyncBroker API ───────────────────────────────────────────────────────

    async def kick(self, message: BrokerMessage) -> None:
        """
        Submit a task to the hub for dispatch.

        :param message: broker message to submit.
        :raises RuntimeError: if the broker has not been started or the hub
            rejects the submission (e.g. queue full).
        """
        if self._ctrl_sock is None:
            raise RuntimeError("Broker is not started")
        payload: dict[str, Any] = {
            "task_id": message.task_id,
            "task_name": message.task_name,
            "payload_b64": base64.b64encode(message.message).decode("ascii"),
            "labels": message.labels,
            "lease_id": "",  # hub assigns the real lease_id at dispatch time
            "priority": int(message.labels.get("priority", 0)),
            "created_at": time.time(),
        }
        resp = await self._send_control("submit", payload)
        if not resp.ok:
            raise RuntimeError(resp.error or "task submission failed")

    async def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        """
        Yield incoming tasks as :class:`~taskiq.acks.AckableMessage` instances.

        Each message's ``ack`` callback sends the hub-issued ``lease_id`` back
        so the hub can validate the ack and reject any late/duplicate ones.

        :raises RuntimeError: if called outside worker mode or before startup.
        :yields: ackable task messages.
        """
        if not self.is_worker_process:
            raise RuntimeError("listen() is only valid in worker mode")
        if self._task_sock is None:
            raise RuntimeError("Task socket is not open (call startup() first)")

        while True:
            try:
                raw = await self._task_sock.arecv()
            except pynng.Closed:
                logger.info("Task socket closed; stopping listen()")
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Task arecv error: %s", exc)
                continue

            try:
                envelope = TaskEnvelope.from_bytes(raw)
            except Exception as exc:
                logger.error("Malformed task envelope discarded: %s", exc)
                continue

            task_id = envelope.task_id
            worker_id = self.worker_id
            lease_id = envelope.lease_id  # hub-assigned; correct by construction

            async def _ack(
                _task_id: str = task_id,
                _worker_id: str = worker_id,
                _lease_id: str = lease_id,
            ) -> None:
                try:
                    resp = await self._send_control(
                        "ack",
                        {
                            "task_id": _task_id,
                            "worker_id": _worker_id,
                            "lease_id": _lease_id,
                        },
                    )
                    if not resp.ok:
                        logger.debug(
                            "Ack rejected for %s (late/duplicate): %s",
                            _task_id, resp.error,
                        )
                except Exception as exc:
                    logger.warning("Ack send failed for %s: %s", _task_id, exc)

            yield AckableMessage(data=envelope.payload, ack=_ack)
