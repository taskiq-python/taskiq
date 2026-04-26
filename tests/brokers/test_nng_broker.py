"""
Tests for the NNG broker, hub, storage, and protocol.

The test suite is split into three layers:

1. **Protocol** — pure serialisation roundtrips; no NNG sockets needed.
2. **Storage** — SQLiteJournal unit tests; no NNG sockets needed.
3. **Integration** — real NNG sockets, real SQLite, single asyncio event loop.
   Uses ``FakeWorker`` / ``FakeClient`` helpers that speak the wire protocol
   directly so we can inject faults precisely (crash before ack, late ack, etc.).

All NNG tests are skipped when ``pynng`` is not installed.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import time
import uuid

import pytest

pynng = pytest.importorskip("pynng")

from taskiq.brokers.nng import (
    HubConfig,
    NNGHub,
    ControlMessage,
    ControlResponse,
    MessageKind,
    TaskEnvelope,
    WorkerState,
    WorkerStatus,
    QueueFullError,
    SQLiteJournal,
    StoreConfig,
)


# ── helpers ───────────────────────────────────────────────────────────────────


def _ipc(tag: str = "") -> str:
    name = f"nng-test-{tag}-{uuid.uuid4().hex[:8]}.ipc"
    return f"ipc://{os.path.join(tempfile.gettempdir(), name)}"


def _envelope(**kwargs: object) -> TaskEnvelope:
    defaults: dict[str, object] = {
        "task_id": uuid.uuid4().hex,
        "task_name": "tests:task",
        "payload_b64": "dGVzdA==",
        "labels": {},
        "lease_id": "",
        "attempts": 0,
        "max_retries": 0,
        "retry_backoff": 1.0,
        "retry_jitter": 0.0,
        "priority": 0,
        "created_at": time.time(),
    }
    defaults.update(kwargs)
    return TaskEnvelope(**defaults)  # type: ignore[arg-type]


def _worker_state(
    worker_id: str | None = None,
    task_addr: str | None = None,
    capacity: int = 2,
) -> WorkerState:
    wid = worker_id or uuid.uuid4().hex
    return WorkerState(
        worker_id=wid,
        task_addr=task_addr or f"ipc:///tmp/{wid}.ipc",
        capacity=capacity,
        heartbeat_interval=5.0,
        lease_timeout=10.0,
    )


def _hub(control_addr: str, db_path: str, **kwargs: object) -> NNGHub:
    cfg = HubConfig(
        control_addr=control_addr,
        task_db=db_path,
        max_pending=100,
        heartbeat_timeout=2.0,
        lease_timeout=2.0,
        dispatch_interval=0.02,
        reaper_interval=0.1,
        control_concurrency=4,
        **kwargs,  # type: ignore[arg-type]
    )
    return NNGHub(cfg)


@pytest.fixture
def db_path(tmp_path: object) -> str:
    import pathlib
    return str(pathlib.Path(str(tmp_path)) / "hub.db")  # type: ignore[arg-type]


@pytest.fixture
def ctrl_addr() -> str:
    return _ipc("ctrl")


class FakeWorker:
    """Minimal NNG worker that speaks the control + task protocol."""

    def __init__(
        self,
        control_addr: str,
        task_addr: str | None = None,
        capacity: int = 1,
    ) -> None:
        self.worker_id = uuid.uuid4().hex[:8]
        self.task_addr = task_addr or _ipc("worker")
        self._ctrl = pynng.Req0(
            dial=control_addr, recv_timeout=3000, send_timeout=3000
        )
        self._pull = pynng.Pull0(listen=self.task_addr, recv_timeout=3000)
        self._lock = asyncio.Lock()
        self.capacity = capacity

    async def ctrl(self, kind: str, payload: dict[str, object]) -> ControlResponse:
        async with self._lock:
            await self._ctrl.asend(
                ControlMessage(kind=kind, payload=payload).to_bytes()
            )
            raw = await self._ctrl.arecv()
        return ControlResponse.from_bytes(raw)

    async def register(self) -> None:
        resp = await self.ctrl(
            "register",
            {
                "worker_id": self.worker_id,
                "task_addr": self.task_addr,
                "capacity": self.capacity,
                "inflight": 0,
                "last_seen": time.time(),
                "heartbeat_interval": 1.0,
                "lease_timeout": 2.0,
                "draining": False,
                "status": str(WorkerStatus.STARTING),
                "version": "test",
            },
        )
        assert resp.ok, f"register failed: {resp.error}"

    async def recv_task(self, timeout: float = 3.0) -> TaskEnvelope:
        raw = await asyncio.wait_for(self._pull.arecv(), timeout=timeout)
        return TaskEnvelope.from_bytes(raw)

    async def ack(self, task_id: str, lease_id: str) -> bool:
        resp = await self.ctrl(
            "ack",
            {
                "task_id": task_id,
                "worker_id": self.worker_id,
                "lease_id": lease_id,
            },
        )
        return resp.ok

    async def heartbeat(self) -> None:
        await self.ctrl("heartbeat", {"worker_id": self.worker_id})

    async def drain_and_unregister(self) -> None:
        await self.ctrl("drain", {"worker_id": self.worker_id})
        await self.ctrl("unregister", {"worker_id": self.worker_id})

    def close(self) -> None:
        self._ctrl.close()
        self._pull.close()


class FakeClient:
    """Minimal NNG client that can submit tasks and query hub status."""

    def __init__(self, control_addr: str) -> None:
        self._ctrl = pynng.Req0(
            dial=control_addr, recv_timeout=3000, send_timeout=3000
        )
        self._lock = asyncio.Lock()

    async def submit(self, **labels: object) -> str:
        tid = uuid.uuid4().hex
        payload: dict[str, object] = {
            "task_id": tid,
            "task_name": "tests:task",
            "payload_b64": "dGVzdA==",
            "labels": {},
            "lease_id": "",
            "attempts": 0,
            "max_retries": labels.pop("max_retries", 0),
            "retry_backoff": labels.pop("retry_backoff", 1.0),
            "retry_jitter": 0.0,
            "priority": labels.pop("priority", 0),
            "created_at": time.time(),
        }
        async with self._lock:
            await self._ctrl.asend(
                ControlMessage(kind="submit", payload=payload).to_bytes()
            )
            raw = await self._ctrl.arecv()
        resp = ControlResponse.from_bytes(raw)
        assert resp.ok, f"submit failed: {resp.error}"
        return tid

    async def ping(self) -> bool:
        async with self._lock:
            await self._ctrl.asend(
                ControlMessage(kind="ping", payload={}).to_bytes()
            )
            raw = await self._ctrl.arecv()
        return ControlResponse.from_bytes(raw).ok

    def close(self) -> None:
        self._ctrl.close()


# ── 1. Protocol tests ─────────────────────────────────────────────────────────


def test_control_message_roundtrip() -> None:
    msg = ControlMessage(kind=MessageKind.HEARTBEAT, payload={"worker_id": "w1"})
    assert ControlMessage.from_bytes(msg.to_bytes()) == msg


def test_control_response_roundtrip() -> None:
    resp = ControlResponse(ok=True, payload={"task_id": "abc"}, error=None)
    assert ControlResponse.from_bytes(resp.to_bytes()) == resp


def test_task_envelope_lease_id_preserved() -> None:
    """Regression: v2 omitted lease_id from the envelope, breaking ack validation."""
    env = TaskEnvelope(
        task_id="x", task_name="m:f", payload_b64="YQ==", lease_id="abc123"
    )
    rt = TaskEnvelope.from_bytes(env.to_bytes())
    assert rt.lease_id == "abc123"


def test_task_envelope_payload_decode() -> None:
    env = _envelope(payload_b64="dGVzdA==")
    assert env.payload == b"test"


# ── 2. Storage tests ──────────────────────────────────────────────────────────


@pytest.fixture
def store(db_path: str) -> SQLiteJournal:
    return SQLiteJournal(StoreConfig(path=db_path, max_pending=50, lease_timeout=5.0))


def test_submit_and_pending(store: SQLiteJournal) -> None:
    store.submit(_envelope())
    assert store.pending_count() == 1


def test_submit_queue_full(db_path: str) -> None:
    s = SQLiteJournal(StoreConfig(path=db_path, max_pending=2))
    s.submit(_envelope())
    s.submit(_envelope())
    with pytest.raises(QueueFullError):
        s.submit(_envelope())


def test_due_tasks_ordered_by_priority(store: SQLiteJournal) -> None:
    store.submit(_envelope(task_id="lo", priority=0))
    store.submit(_envelope(task_id="hi", priority=10))
    due = store.due_tasks(limit=10)
    assert due[0]["task_id"] == "hi"
    assert due[1]["task_id"] == "lo"


def test_ack_happy_path(store: SQLiteJournal) -> None:
    env = _envelope()
    store.submit(env)
    w = _worker_state()
    store.register_worker(w)
    assert store.mark_leased(env.task_id, w.worker_id, "L1", time.time() + 60)
    assert store.ack(env.task_id, w.worker_id, "L1")
    assert store.get_task(env.task_id)["state"] == "done"


def test_ack_wrong_lease_rejected(store: SQLiteJournal) -> None:
    env = _envelope()
    store.submit(env)
    w = _worker_state()
    store.register_worker(w)
    store.mark_leased(env.task_id, w.worker_id, "real", time.time() + 60)
    assert not store.ack(env.task_id, w.worker_id, "wrong")


def test_late_ack_after_requeue_ignored(store: SQLiteJournal) -> None:
    env = _envelope()
    store.submit(env)
    w = _worker_state()
    store.register_worker(w)
    store.mark_leased(env.task_id, w.worker_id, "L2", time.time() - 1)
    assert store.reap_expired_leases() == 1
    assert not store.ack(env.task_id, w.worker_id, "L2")


def test_nack_requeues_with_backoff(store: SQLiteJournal) -> None:
    env = _envelope(max_retries=2, retry_backoff=1.0)
    store.submit(env)
    w = _worker_state()
    store.register_worker(w)
    store.mark_leased(env.task_id, w.worker_id, "L3", time.time() + 60)
    assert store.nack(env.task_id, w.worker_id, "L3", "boom")
    task = store.get_task(env.task_id)
    assert task["state"] == "ready"
    assert float(task["next_run_at"]) > time.time()


def test_nack_exceeds_retries_fails(store: SQLiteJournal) -> None:
    env = _envelope(max_retries=0)
    store.submit(env)
    w = _worker_state()
    store.register_worker(w)
    store.mark_leased(env.task_id, w.worker_id, "L4", time.time() + 60)
    store.nack(env.task_id, w.worker_id, "L4", "error")
    assert store.get_task(env.task_id)["state"] == "failed"


def test_dead_worker_tasks_requeued(store: SQLiteJournal, db_path: str) -> None:
    w = _worker_state()
    store.register_worker(w)
    env = _envelope(max_retries=3)
    store.submit(env)
    store.mark_leased(env.task_id, w.worker_id, "L5", time.time() + 60)
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE workers SET last_seen=0 WHERE worker_id=?", (w.worker_id,))
    conn.commit()
    conn.close()
    assert store.recover_dead_workers(heartbeat_timeout=1.0) == 1
    assert store.get_task(env.task_id)["state"] == "ready"


def test_choose_worker_least_loaded(store: SQLiteJournal, db_path: str) -> None:
    w1 = _worker_state(worker_id="w1", capacity=4)
    w2 = _worker_state(worker_id="w2", capacity=4)
    store.register_worker(w1)
    store.register_worker(w2)
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE workers SET inflight=3 WHERE worker_id='w1'")
    conn.commit()
    conn.close()
    chosen = store.choose_worker("least_loaded", heartbeat_timeout=30.0)
    assert chosen is not None
    assert chosen["worker_id"] == "w2"


def test_stats(store: SQLiteJournal) -> None:
    w = _worker_state()
    store.register_worker(w)
    store.submit(_envelope())
    s = store.stats()
    assert s["ready"] == 1
    assert s["active_workers"] == 1


# ── 3. Integration tests ──────────────────────────────────────────────────────


async def test_ping(ctrl_addr: str, db_path: str) -> None:
    hub = _hub(ctrl_addr, db_path)
    await hub.start()
    client = FakeClient(ctrl_addr)
    try:
        assert await client.ping()
    finally:
        client.close()
        await hub.stop()


async def test_submit_dispatch_ack(ctrl_addr: str, db_path: str) -> None:
    """Golden path: one task, one worker, full round-trip."""
    hub = _hub(ctrl_addr, db_path)
    await hub.start()
    worker = FakeWorker(ctrl_addr, capacity=1)
    client = FakeClient(ctrl_addr)
    try:
        await worker.register()
        tid = await client.submit()
        env = await worker.recv_task(timeout=3.0)
        assert env.task_id == tid
        assert env.lease_id != "", "Hub must populate lease_id in envelope"
        assert await worker.ack(env.task_id, env.lease_id)
        assert hub.store.get_task(tid)["state"] == "done"
    finally:
        worker.close()
        client.close()
        await hub.stop()


async def test_multiple_workers_load_balanced(ctrl_addr: str, db_path: str) -> None:
    """Both workers must receive at least one task — no single hot-spot."""
    hub = _hub(ctrl_addr, db_path)
    await hub.start()
    w1 = FakeWorker(ctrl_addr, capacity=4)
    w2 = FakeWorker(ctrl_addr, capacity=4)
    client = FakeClient(ctrl_addr)
    try:
        await w1.register()
        await w2.register()
        task_ids = [await client.submit() for _ in range(6)]
        received: dict[str, list[str]] = {w1.worker_id: [], w2.worker_id: []}
        pending = set(task_ids)

        async def drain(w: FakeWorker) -> None:
            while pending:
                try:
                    env = await w.recv_task(timeout=0.5)
                    received[w.worker_id].append(env.task_id)
                    pending.discard(env.task_id)
                    await w.ack(env.task_id, env.lease_id)
                except asyncio.TimeoutError:
                    break

        await asyncio.gather(drain(w1), drain(w2))
        assert not pending, f"Tasks not delivered: {pending}"
        assert len(received[w1.worker_id]) > 0
        assert len(received[w2.worker_id]) > 0
    finally:
        w1.close()
        w2.close()
        client.close()
        await hub.stop()


async def test_worker_crash_before_ack_task_requeued(
    ctrl_addr: str, db_path: str
) -> None:
    """
    Worker receives a task but dies before acking.
    After lease expiry the hub must requeue it for a second worker.
    """
    hub = _hub(ctrl_addr, db_path)
    await hub.start()
    w1 = FakeWorker(ctrl_addr, capacity=1)
    client = FakeClient(ctrl_addr)
    try:
        await w1.register()
        tid = await client.submit(max_retries=3)
        env1 = await w1.recv_task(timeout=3.0)
        assert env1.task_id == tid
        w1.close()  # simulate crash without acking

        await asyncio.sleep(3.5)  # lease_timeout=2s + reaper_interval=0.1s

        assert hub.store.get_task(tid)["state"] == "ready"

        w2 = FakeWorker(ctrl_addr, capacity=1)
        try:
            await w2.register()
            env2 = await w2.recv_task(timeout=3.0)
            assert env2.task_id == tid
            assert env2.lease_id != env1.lease_id
            assert await w2.ack(env2.task_id, env2.lease_id)
            assert hub.store.get_task(tid)["state"] == "done"
        finally:
            w2.close()
    finally:
        client.close()
        await hub.stop()


async def test_late_ack_after_requeue_rejected(
    ctrl_addr: str, db_path: str
) -> None:
    """
    Sequence: dispatch to w1 → lease expires → requeue → dispatch to w2.
    w1's late ack must be rejected; w2's ack must succeed.
    """
    hub = _hub(ctrl_addr, db_path)
    await hub.start()
    w1 = FakeWorker(ctrl_addr, capacity=1)
    client = FakeClient(ctrl_addr)
    try:
        await w1.register()
        tid = await client.submit(max_retries=3)
        env1 = await w1.recv_task(timeout=3.0)
        await asyncio.sleep(3.5)  # let lease expire

        w2 = FakeWorker(ctrl_addr, capacity=1)
        try:
            await w2.register()
            env2 = await w2.recv_task(timeout=3.0)

            # w1's stale ack must be rejected
            assert not await w1.ack(env1.task_id, env1.lease_id)
            # w2's valid ack succeeds
            assert await w2.ack(env2.task_id, env2.lease_id)
            assert hub.store.get_task(tid)["state"] == "done"
        finally:
            w2.close()
    finally:
        w1.close()
        client.close()
        await hub.stop()


async def test_hub_restart_recovers_orphaned_tasks(
    ctrl_addr: str, db_path: str
) -> None:
    """
    Tasks leased at hub shutdown must be requeued when a new hub starts
    with the same database.
    """
    hub1 = _hub(ctrl_addr, db_path)
    await hub1.start()
    w1 = FakeWorker(ctrl_addr, capacity=1)
    client = FakeClient(ctrl_addr)
    await w1.register()
    tid = await client.submit(max_retries=3)
    env = await w1.recv_task(timeout=3.0)
    assert env.task_id == tid
    # "kill" hub1 without giving worker a chance to ack
    await hub1.stop()
    w1.close()
    client.close()

    # Task is still leased in the DB
    assert hub1.store.get_task(tid)["state"] == "leased"

    hub2 = _hub(ctrl_addr, db_path)
    await hub2.start()
    await asyncio.sleep(0.3)  # allow startup recovery
    try:
        assert hub2.store.get_task(tid)["state"] == "ready"
    finally:
        await hub2.stop()


async def test_concurrent_heartbeats(ctrl_addr: str, db_path: str) -> None:
    """
    N workers heartbeat simultaneously.  With concurrent Rep0 contexts all
    must succeed without serialisation stalls.
    """
    hub = _hub(ctrl_addr, db_path)
    await hub.start()
    workers = [FakeWorker(ctrl_addr, capacity=2) for _ in range(8)]
    try:
        await asyncio.gather(*[w.register() for w in workers])
        results = await asyncio.gather(
            *[w.heartbeat() for w in workers],
            return_exceptions=True,
        )
        errors = [r for r in results if isinstance(r, Exception)]
        assert not errors, f"Concurrent heartbeats failed: {errors}"
    finally:
        for w in workers:
            w.close()
        await hub.stop()


async def test_graceful_drain_and_unregister(ctrl_addr: str, db_path: str) -> None:
    hub = _hub(ctrl_addr, db_path)
    await hub.start()
    worker = FakeWorker(ctrl_addr, capacity=2)
    try:
        await worker.register()
        assert len(hub.store.list_workers()) == 1
        await worker.drain_and_unregister()
        await asyncio.sleep(0.1)
        assert len(hub.store.list_workers()) == 0
    finally:
        worker.close()
        await hub.stop()
