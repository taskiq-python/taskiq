"""Durable WAL-mode SQLite task journal for the NNG hub."""
from __future__ import annotations

import json
import random
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator

from protocol import TaskEnvelope, WorkerState, WorkerStatus


@dataclass
class StoreConfig:
    """Configuration for the SQLite task journal."""

    path: str
    max_pending: int = 10_000
    lease_timeout: float = 30.0
    backoff_base: float = 1.0
    backoff_cap: float = 60.0


class QueueFullError(RuntimeError):
    """Raised when a submission is attempted on a full queue."""


class SQLiteJournal:
    """
    Thread-safe, WAL-mode SQLite task store.

    Design notes
    ────────────
    * Every method opens and closes its own connection.  WAL allows concurrent
      readers without blocking; SQLite serialises concurrent writers internally,
      and the Python-level ``_submit_lock`` prevents the TOCTOU race in
      :meth:`submit`.
    * The hub runs every call through a single-threaded
      ``ThreadPoolExecutor`` so, in practice, writes never contend at the
      OS level either.
    * ``PRAGMA`` settings (WAL, synchronous, busy_timeout) are applied per
      connection because each ``sqlite3.connect()`` call starts with defaults.
    """

    def __init__(self, config: StoreConfig) -> None:
        """Initialise the journal and create schema if not present."""
        self.config = config
        # Guards only the pending_count check + INSERT pair in submit() to
        # prevent concurrent callers from racing past max_pending.
        self._submit_lock = threading.Lock()
        self._init()

    # ── connection ────────────────────────────────────────────────────────────

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(
            self.config.path,
            timeout=10.0,
            check_same_thread=False,
            isolation_level=None,  # we manage transactions explicitly
        )
        conn.row_factory = sqlite3.Row
        # Must be set per-connection, not just once at schema creation.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")  # safe with WAL; faster than FULL
        conn.execute("PRAGMA busy_timeout=5000")  # wait up to 5s before SQLITE_BUSY
        conn.execute("PRAGMA cache_size=-32000")  # 32 MB page cache
        try:
            yield conn
        finally:
            conn.close()

    def _init(self) -> None:
        Path(self.config.path).parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id          TEXT    PRIMARY KEY,
                    task_name        TEXT    NOT NULL,
                    payload          BLOB    NOT NULL,
                    labels_json      TEXT    NOT NULL DEFAULT '{}',
                    state            TEXT    NOT NULL,
                    attempts         INTEGER NOT NULL DEFAULT 0,
                    max_retries      INTEGER NOT NULL DEFAULT 0,
                    retry_backoff    REAL    NOT NULL DEFAULT 1.0,
                    retry_jitter     REAL    NOT NULL DEFAULT 0.0,
                    priority         INTEGER NOT NULL DEFAULT 0,
                    created_at       REAL    NOT NULL,
                    updated_at       REAL    NOT NULL,
                    next_run_at      REAL    NOT NULL,
                    lease_id         TEXT,
                    leased_worker_id TEXT,
                    lease_until      REAL,
                    last_error       TEXT
                );

                CREATE TABLE IF NOT EXISTS workers (
                    worker_id          TEXT    PRIMARY KEY,
                    task_addr          TEXT    NOT NULL,
                    capacity           INTEGER NOT NULL,
                    inflight           INTEGER NOT NULL DEFAULT 0,
                    last_seen          REAL    NOT NULL DEFAULT 0,
                    heartbeat_interval REAL    NOT NULL DEFAULT 5.0,
                    lease_timeout      REAL    NOT NULL DEFAULT 15.0,
                    draining           INTEGER NOT NULL DEFAULT 0,
                    status             TEXT    NOT NULL,
                    version            TEXT    NOT NULL DEFAULT 'unknown'
                );

                CREATE TABLE IF NOT EXISTS journal (
                    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts           REAL    NOT NULL,
                    kind         TEXT    NOT NULL,
                    payload_json TEXT    NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_dispatch
                    ON tasks (state, next_run_at, priority DESC);
                CREATE INDEX IF NOT EXISTS idx_tasks_lease
                    ON tasks (state, lease_until);
                CREATE INDEX IF NOT EXISTS idx_workers_active
                    ON workers (status, draining, last_seen);
            """)

    # ── helpers ───────────────────────────────────────────────────────────────

    def _journal(
        self,
        conn: sqlite3.Connection,
        kind: str,
        payload: dict[str, Any],
    ) -> None:
        conn.execute(
            "INSERT INTO journal (ts, kind, payload_json) VALUES (?, ?, ?)",
            (
                time.time(),
                kind,
                json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
            ),
        )

    def _backoff(self, attempts: int, backoff_base: float) -> float:
        return min(self.config.backoff_cap, backoff_base * (2 ** max(0, attempts - 1)))

    # ── task lifecycle ────────────────────────────────────────────────────────

    def pending_count(self) -> int:
        """Return the number of ready + leased tasks."""
        with self._conn() as conn:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM tasks WHERE state IN ('ready', 'leased')",
                ).fetchone()[0],
            )

    def submit(self, envelope: TaskEnvelope) -> None:
        """
        Persist a new task in 'ready' state.

        :param envelope: task envelope to store.
        :raises QueueFullError: when ``max_pending`` is reached.
        """
        now = time.time()
        with self._submit_lock, self._conn() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE state IN ('ready', 'leased')",
            ).fetchone()[0]
            if count >= self.config.max_pending:
                raise QueueFullError("Task queue is full.")
            conn.execute("BEGIN")
            conn.execute(
                """
                INSERT INTO tasks (
                    task_id, task_name, payload, labels_json, state,
                    attempts, max_retries, retry_backoff, retry_jitter,
                    priority, created_at, updated_at, next_run_at
                ) VALUES (?, ?, ?, ?, 'ready', 0, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    envelope.task_id,
                    envelope.task_name,
                    envelope.payload,
                    json.dumps(
                        envelope.labels, separators=(",", ":"), ensure_ascii=False
                    ),
                    envelope.max_retries,
                    envelope.retry_backoff,
                    envelope.retry_jitter,
                    envelope.priority,
                    envelope.created_at or now,
                    now,
                    now,
                ),
            )
            self._journal(
                conn,
                "task_submitted",
                {"task_id": envelope.task_id, "task_name": envelope.task_name},
            )
            conn.execute("COMMIT")

    def due_tasks(self, limit: int = 50) -> list[sqlite3.Row]:
        """
        Return ready tasks whose ``next_run_at`` is in the past.

        Results are ordered by priority (descending) then creation time.

        :param limit: maximum number of rows to return.
        :return: list of task rows.
        """
        now = time.time()
        with self._conn() as conn:
            return list(
                conn.execute(
                    """
                    SELECT * FROM tasks
                    WHERE state = 'ready' AND next_run_at <= ?
                    ORDER BY priority DESC, created_at ASC
                    LIMIT ?
                    """,
                    (now, limit),
                ),
            )

    def mark_leased(
        self,
        task_id: str,
        worker_id: str,
        lease_id: str,
        lease_until: float,
    ) -> bool:
        """
        Atomically transition a task from 'ready' to 'leased'.

        :param task_id: task to lease.
        :param worker_id: worker receiving the task.
        :param lease_id: unique token for this dispatch attempt.
        :param lease_until: absolute epoch deadline for the lease.
        :return: True if the transition succeeded; False if the task was
                 already taken (concurrent dispatch race).
        """
        now = time.time()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT state FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if not row or row["state"] != "ready":
                return False
            conn.execute("BEGIN")
            conn.execute(
                """
                UPDATE tasks
                SET state = 'leased',
                    leased_worker_id = ?, lease_id = ?, lease_until = ?,
                    attempts = attempts + 1, updated_at = ?
                WHERE task_id = ?
                """,
                (worker_id, lease_id, lease_until, now, task_id),
            )
            conn.execute(
                "UPDATE workers SET inflight = inflight + 1 WHERE worker_id = ?",
                (worker_id,),
            )
            self._journal(
                conn,
                "task_leased",
                {
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "lease_id": lease_id,
                },
            )
            conn.execute("COMMIT")
            return True

    def ack(self, task_id: str, worker_id: str, lease_id: str) -> bool:
        """
        Mark a task as successfully completed.

        Late or duplicate acks (mismatched ``lease_id`` or state ≠ 'leased')
        are silently rejected and return False.

        :param task_id: task being acknowledged.
        :param worker_id: worker sending the ack.
        :param lease_id: lease token that was issued at dispatch.
        :return: True if the ack was accepted.
        """
        now = time.time()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT state, lease_id, leased_worker_id FROM tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if not row or row["state"] != "leased":
                return False
            if row["lease_id"] != lease_id or row["leased_worker_id"] != worker_id:
                return False
            conn.execute("BEGIN")
            conn.execute(
                """
                UPDATE tasks
                SET state = 'done', updated_at = ?,
                    lease_id = NULL, leased_worker_id = NULL, lease_until = NULL
                WHERE task_id = ?
                """,
                (now, task_id),
            )
            conn.execute(
                "UPDATE workers SET inflight = MAX(inflight - 1, 0) WHERE worker_id = ?",
                (worker_id,),
            )
            self._journal(
                conn,
                "task_acked",
                {
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "lease_id": lease_id,
                },
            )
            conn.execute("COMMIT")
            return True

    def nack(
        self, task_id: str, worker_id: str, lease_id: str, error: str
    ) -> bool:
        """
        Explicitly fail a task, triggering retry or permanent failure.

        :param task_id: task being nacked.
        :param worker_id: worker sending the nack.
        :param lease_id: lease token issued at dispatch.
        :param error: human-readable reason for the failure.
        :return: True if the nack was accepted.
        """
        return self._requeue_or_fail(task_id, worker_id, lease_id, error)

    def _requeue_or_fail(
        self, task_id: str, worker_id: str, lease_id: str, error: str
    ) -> bool:
        now = time.time()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if (
                not row
                or row["state"] != "leased"
                or row["lease_id"] != lease_id
                or row["leased_worker_id"] != worker_id
            ):
                return False
            attempts = int(row["attempts"])
            max_retries = int(row["max_retries"])
            conn.execute("BEGIN")
            if attempts > max_retries:
                conn.execute(
                    """
                    UPDATE tasks
                    SET state = 'failed', updated_at = ?,
                        lease_id = NULL, leased_worker_id = NULL, lease_until = NULL,
                        last_error = ?
                    WHERE task_id = ?
                    """,
                    (now, error, task_id),
                )
            else:
                backoff = self._backoff(attempts, float(row["retry_backoff"]))
                conn.execute(
                    """
                    UPDATE tasks
                    SET state = 'ready', updated_at = ?, next_run_at = ?,
                        lease_id = NULL, leased_worker_id = NULL, lease_until = NULL,
                        last_error = ?
                    WHERE task_id = ?
                    """,
                    (now, now + backoff, error, task_id),
                )
            conn.execute(
                "UPDATE workers SET inflight = MAX(inflight - 1, 0) WHERE worker_id = ?",
                (worker_id,),
            )
            self._journal(
                conn,
                "task_nacked",
                {
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "lease_id": lease_id,
                    "error": error,
                    "requeued": attempts <= max_retries,
                },
            )
            conn.execute("COMMIT")
            return True

    # ── reaper / recovery ─────────────────────────────────────────────────────

    def reap_expired_leases(self) -> int:
        """
        Find leases past their deadline and requeue or permanently fail them.

        :return: number of tasks reaped.
        """
        now = time.time()
        with self._conn() as conn:
            expired = list(
                conn.execute(
                    """
                    SELECT * FROM tasks
                    WHERE state = 'leased'
                      AND lease_until IS NOT NULL
                      AND lease_until < ?
                    """,
                    (now,),
                ),
            )
            if not expired:
                return 0
            conn.execute("BEGIN")
            count = 0
            for row in expired:
                attempts = int(row["attempts"])
                max_retries = int(row["max_retries"])
                worker_id = row["leased_worker_id"]
                if attempts > max_retries:
                    conn.execute(
                        """
                        UPDATE tasks
                        SET state = 'failed', updated_at = ?,
                            lease_id = NULL, leased_worker_id = NULL, lease_until = NULL,
                            last_error = 'lease expired'
                        WHERE task_id = ?
                        """,
                        (now, row["task_id"]),
                    )
                else:
                    backoff = self._backoff(attempts, float(row["retry_backoff"]))
                    conn.execute(
                        """
                        UPDATE tasks
                        SET state = 'ready', updated_at = ?, next_run_at = ?,
                            lease_id = NULL, leased_worker_id = NULL, lease_until = NULL,
                            last_error = 'lease expired'
                        WHERE task_id = ?
                        """,
                        (now, now + backoff, row["task_id"]),
                    )
                if worker_id:
                    conn.execute(
                        "UPDATE workers SET inflight = MAX(inflight - 1, 0) WHERE worker_id = ?",
                        (worker_id,),
                    )
                self._journal(
                    conn,
                    "lease_reaped",
                    {
                        "task_id": row["task_id"],
                        "worker_id": worker_id,
                        "lease_id": row["lease_id"],
                    },
                )
                count += 1
            conn.execute("COMMIT")
            return count

    def recover_dead_workers(self, heartbeat_timeout: float) -> int:
        """
        Mark workers that missed their heartbeat deadline as DEAD.

        All tasks leased to dead workers are requeued (or permanently failed
        if retries are exhausted).

        :param heartbeat_timeout: seconds of silence before a worker is dead.
        :return: number of tasks requeued.
        """
        now = time.time()
        cutoff = now - heartbeat_timeout
        with self._conn() as conn:
            dead = list(
                conn.execute(
                    "SELECT * FROM workers WHERE last_seen < ? AND status != 'dead'",
                    (cutoff,),
                ),
            )
            if not dead:
                return 0
            conn.execute("BEGIN")
            requeued = 0
            for worker in dead:
                worker_id = worker["worker_id"]
                conn.execute(
                    "UPDATE workers SET status = 'dead', draining = 1 WHERE worker_id = ?",
                    (worker_id,),
                )
                leased = list(
                    conn.execute(
                        "SELECT * FROM tasks WHERE state = 'leased' AND leased_worker_id = ?",
                        (worker_id,),
                    ),
                )
                for row in leased:
                    attempts = int(row["attempts"])
                    max_retries = int(row["max_retries"])
                    if attempts > max_retries:
                        conn.execute(
                            """
                            UPDATE tasks
                            SET state = 'failed', updated_at = ?,
                                lease_id = NULL, leased_worker_id = NULL, lease_until = NULL,
                                last_error = 'worker died'
                            WHERE task_id = ?
                            """,
                            (now, row["task_id"]),
                        )
                    else:
                        backoff = self._backoff(
                            attempts, float(row["retry_backoff"])
                        )
                        conn.execute(
                            """
                            UPDATE tasks
                            SET state = 'ready', updated_at = ?, next_run_at = ?,
                                lease_id = NULL, leased_worker_id = NULL, lease_until = NULL,
                                last_error = 'worker died'
                            WHERE task_id = ?
                            """,
                            (now, now + backoff, row["task_id"]),
                        )
                    self._journal(
                        conn,
                        "worker_dead_requeue",
                        {"worker_id": worker_id, "task_id": row["task_id"]},
                    )
                    requeued += 1
            conn.execute("COMMIT")
            return requeued

    # ── worker lifecycle ──────────────────────────────────────────────────────

    def register_worker(self, worker: WorkerState) -> None:
        """
        Upsert a worker record.

        Re-registering an existing worker (e.g. after hub restart) resets
        its draining flag and updates its metadata.

        :param worker: worker state snapshot from the registration message.
        """
        now = time.time()
        with self._conn() as conn:
            conn.execute("BEGIN")
            conn.execute(
                """
                INSERT INTO workers (
                    worker_id, task_addr, capacity, inflight, last_seen,
                    heartbeat_interval, lease_timeout, draining, status, version
                ) VALUES (?, ?, ?, 0, ?, ?, ?, 0, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    task_addr          = excluded.task_addr,
                    capacity           = excluded.capacity,
                    last_seen          = excluded.last_seen,
                    heartbeat_interval = excluded.heartbeat_interval,
                    lease_timeout      = excluded.lease_timeout,
                    draining           = 0,
                    status             = excluded.status,
                    version            = excluded.version
                """,
                (
                    worker.worker_id,
                    worker.task_addr,
                    worker.capacity,
                    now,
                    worker.heartbeat_interval,
                    worker.lease_timeout,
                    str(WorkerStatus.LISTENING),
                    worker.version,
                ),
            )
            self._journal(
                conn,
                "worker_register",
                {"worker_id": worker.worker_id, "task_addr": worker.task_addr},
            )
            conn.execute("COMMIT")

    def heartbeat(self, worker_id: str) -> None:
        """
        Record a heartbeat from a worker, resetting its last_seen timestamp.

        :param worker_id: ID of the worker sending the heartbeat.
        """
        with self._conn() as conn:
            conn.execute("BEGIN")
            conn.execute(
                "UPDATE workers SET last_seen = ?, status = ? WHERE worker_id = ?",
                (time.time(), str(WorkerStatus.LISTENING), worker_id),
            )
            self._journal(conn, "heartbeat", {"worker_id": worker_id})
            conn.execute("COMMIT")

    def unregister_worker(self, worker_id: str) -> None:
        """
        Remove a worker from the registry (graceful shutdown path).

        :param worker_id: ID of the worker unregistering.
        """
        with self._conn() as conn:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM workers WHERE worker_id = ?", (worker_id,))
            self._journal(conn, "worker_unregister", {"worker_id": worker_id})
            conn.execute("COMMIT")

    def mark_draining(self, worker_id: str) -> None:
        """
        Mark a worker as draining so the hub stops dispatching new tasks to it.

        :param worker_id: ID of the worker entering drain mode.
        """
        with self._conn() as conn:
            conn.execute("BEGIN")
            conn.execute(
                "UPDATE workers SET draining = 1, status = ? WHERE worker_id = ?",
                (str(WorkerStatus.DRAINING), worker_id),
            )
            self._journal(conn, "worker_drain", {"worker_id": worker_id})
            conn.execute("COMMIT")

    # ── routing ───────────────────────────────────────────────────────────────

    def choose_worker(
        self,
        routing_policy: str = "least_loaded",
        *,
        heartbeat_timeout: float = 15.0,
    ) -> sqlite3.Row | None:
        """
        Select the best available worker according to ``routing_policy``.

        ``'least_loaded'`` picks the worker with the lowest ``inflight /
        capacity`` ratio.  ``'p2c'`` (Power-of-Two-Choices) samples two
        workers at random and picks the less loaded one, reducing hot-spot
        probability under high concurrency.

        :param routing_policy: ``'least_loaded'`` or ``'p2c'``.
        :param heartbeat_timeout: seconds before a worker is considered stale.
        :return: chosen worker row, or None if no worker has capacity.
        """
        cutoff = time.time() - heartbeat_timeout
        with self._conn() as conn:
            rows = list(
                conn.execute(
                    """
                    SELECT * FROM workers
                    WHERE status IN ('starting', 'listening')
                      AND draining = 0
                      AND last_seen >= ?
                    """,
                    (cutoff,),
                ),
            )
        available = [
            w for w in rows if int(w["inflight"]) < int(w["capacity"])
        ]
        if not available:
            return None
        if routing_policy == "p2c" and len(available) >= 2:
            a, b = random.sample(available, k=2)  # noqa: S311
            load_a = int(a["inflight"]) / max(int(a["capacity"]), 1)
            load_b = int(b["inflight"]) / max(int(b["capacity"]), 1)
            return a if load_a <= load_b else b
        return min(
            available,
            key=lambda w: int(w["inflight"]) / max(int(w["capacity"]), 1),
        )

    # ── management / observability ────────────────────────────────────────────

    def get_task(self, task_id: str) -> sqlite3.Row | None:
        """
        Fetch a single task row by ID.

        :param task_id: ID of the task to look up.
        :return: row or None if not found.
        """
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()

    def list_workers(self) -> list[sqlite3.Row]:
        """Return all registered workers ordered by most-recently-seen."""
        with self._conn() as conn:
            return list(
                conn.execute("SELECT * FROM workers ORDER BY last_seen DESC"),
            )

    def stats(self) -> dict[str, int]:
        """Return a summary dict with task state counts and active worker count."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) AS n FROM tasks GROUP BY state",
            ).fetchall()
            counts = {r["state"]: r["n"] for r in rows}
            worker_count = conn.execute(
                """
                SELECT COUNT(*) FROM workers
                WHERE status IN ('starting', 'listening') AND draining = 0
                """,
            ).fetchone()[0]
        return {
            "ready": counts.get("ready", 0),
            "leased": counts.get("leased", 0),
            "done": counts.get("done", 0),
            "failed": counts.get("failed", 0),
            "active_workers": worker_count,
        }
