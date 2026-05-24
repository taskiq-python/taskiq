"""Pure in-memory task store for the NNG hub — no external dependencies."""
from __future__ import annotations

import functools
import inspect
import random
import time
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .protocol import TaskEnvelope, WorkerState


@dataclass
class StoreConfig:
    """Configuration for :class:`InMemoryStore`."""

    path: str = ""  # kept for API compat; not used
    max_pending: int = 10_000
    lease_timeout: float = 30.0
    # Hub-internal cap on delivery retries (lease expiry / worker death).
    # Has nothing to do with user-level retry policy, which is handled by
    # taskiq retry middlewares.  Set to 0 to disable redelivery entirely.
    max_delivery_attempts: int = 5
    delivery_backoff: float = 1.0
    backoff_cap: float = 60.0


class QueueFullError(RuntimeError):
    """Raised when a submission is attempted on a full queue."""


@dataclass
class _Task:
    task_id: str
    task_name: str
    payload: bytes
    labels: dict[str, Any]
    state: str  # ready / leased / done / failed
    # Internal delivery-attempt counter (incremented on each dispatch).
    # NOT related to user-level retry policy — that lives in middlewares.
    attempts: int = 0
    priority: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    next_run_at: float = field(default_factory=time.time)
    lease_id: str | None = None
    leased_worker_id: str | None = None
    lease_until: float | None = None
    last_error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return a dict view of this task record."""
        return asdict(self)

    def as_status_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict (no raw bytes) for control-plane status responses."""
        d = self.as_dict()
        d.pop("payload", None)
        return d


@dataclass
class _Worker:
    worker_id: str
    task_addr: str
    capacity: int
    inflight: int = 0
    last_seen: float = 0.0
    heartbeat_interval: float = 5.0
    lease_timeout: float = 15.0
    draining: bool = False
    status: str = "starting"
    version: str = "unknown"

    def as_dict(self) -> dict[str, Any]:
        """Return a dict view of this worker record."""
        return asdict(self)


# ── task context ─────────────────────────────────────────────────────────────


@dataclass
class TaskContext:
    """Task metadata passed to context-aware routing policies (e.g. affinity)."""

    task_id: str
    task_name: str
    labels: dict[str, Any]
    priority: int = 0
    attempts: int = 0


# ── routing policy abstraction ────────────────────────────────────────────────


@dataclass(frozen=True)
class WorkerView:
    """Immutable worker snapshot passed to :class:`RoutingPolicy` implementations."""

    worker_id: str
    inflight: int
    capacity: int

    @property
    def load(self) -> float:
        """Fractional load: 0.0 idle -> 1.0 at capacity."""
        return self.inflight / max(self.capacity, 1)


@runtime_checkable
class RoutingPolicy(Protocol):
    """Strategy interface for selecting a dispatch target from available workers."""

    def choose(self, workers: list[WorkerView]) -> WorkerView | None:
        """Return the chosen worker, or None to hold off dispatch."""
        ...


class LeastLoaded:
    """Pick the worker with the lowest inflight / capacity ratio."""

    def choose(self, workers: list[WorkerView]) -> WorkerView | None:
        """Return the least-loaded worker."""
        if not workers:
            return None
        return min(workers, key=lambda w: w.load)


class PowerOfTwoChoices:
    """
    Power-of-two-choices routing.

    Samples two workers uniformly at random and returns the less loaded one.
    Reduces hot-spot probability under high concurrency compared to pure random.
    """

    def choose(self, workers: list[WorkerView]) -> WorkerView | None:
        """Return the less loaded of two randomly sampled workers."""
        if not workers:
            return None
        if len(workers) == 1:
            return workers[0]
        a, b = random.sample(workers, k=2)  # noqa: S311
        return a if a.load <= b.load else b


class RoundRobin:
    """
    Round-robin routing — cycle through workers in alphabetical ID order.

    Ignores load; useful when tasks are homogeneous and worker capacity is equal.
    The counter is per-instance, so each :class:`NNGHub` maintains its own cycle.
    """

    def __init__(self) -> None:
        """Initialise the cycle counter."""
        self._idx: int = 0

    def choose(self, workers: list[WorkerView]) -> WorkerView | None:
        """Return the next worker in the cycle."""
        if not workers:
            return None
        w = workers[self._idx % len(workers)]
        self._idx += 1
        return w


class AffinityPolicy:
    """
    Sticky routing: tasks with the same ``affinity_key`` label always go to the
    same worker.  Falls back to least-loaded when the preferred worker is gone.

    The affinity table is per-instance and lives only in memory.
    """

    def __init__(self) -> None:
        """Initialise an empty affinity table."""
        self._table: dict[str, str] = {}  # affinity_key -> worker_id

    def choose(
        self,
        workers: list[WorkerView],
        task: "TaskContext | None" = None,
    ) -> WorkerView | None:
        """Return the sticky worker for the task's affinity key, or least-loaded."""
        if not workers:
            return None
        if task is not None:
            key = str(task.labels.get("affinity_key", ""))
            if key and key in self._table:
                match = next(
                    (w for w in workers if w.worker_id == self._table[key]), None
                )
                if match is not None:
                    return match
        chosen = min(workers, key=lambda w: w.load)
        if task is not None:
            key = str(task.labels.get("affinity_key", ""))
            if key:
                self._table[key] = chosen.worker_id
        return chosen


@functools.lru_cache(maxsize=None)
def _policy_accepts_task(policy_cls: type) -> bool:
    """Return True if policy.choose accepts a ``task`` keyword argument."""
    try:
        return "task" in inspect.signature(policy_cls.choose).parameters
    except (ValueError, TypeError):
        return False


def _choose_with_context(
    policy: RoutingPolicy,
    views: list[WorkerView],
    task: "TaskContext | None",
) -> "WorkerView | None":
    """Call policy.choose, passing ``task`` only when the policy supports it."""
    if task is not None and _policy_accepts_task(type(policy)):
        return policy.choose(views, task=task)  # type: ignore[call-arg]
    return policy.choose(views)


# Singletons for stateless built-ins; RoundRobin/AffinityPolicy singletons are
# fine for single-hub processes.  Users needing isolated state should pass their
# own instance.
_BUILTIN_POLICIES: dict[str, RoutingPolicy] = {
    "least_loaded": LeastLoaded(),
    "p2c": PowerOfTwoChoices(),
    "round_robin": RoundRobin(),
    "affinity": AffinityPolicy(),  # type: ignore[dict-item]
}


def make_routing_policy(policy: "RoutingPolicy | str") -> RoutingPolicy:
    """
    Resolve a routing policy name or pass through an instance.

    :param policy: ``'least_loaded'``, ``'p2c'``, ``'round_robin'``, or a
        :class:`RoutingPolicy` instance.
    :return: concrete routing policy.
    :raises ValueError: for unknown string names.
    """
    if isinstance(policy, str):
        resolved = _BUILTIN_POLICIES.get(policy)
        if resolved is None:
            raise ValueError(
                f"Unknown routing policy {policy!r}; "
                f"available: {sorted(_BUILTIN_POLICIES)}"
            )
        return resolved
    return policy


# ── scheduler abstraction ─────────────────────────────────────────────────────


@runtime_checkable
class Scheduler(Protocol):
    """Strategy interface for selecting which tasks to dispatch next."""

    def select(self, store: "InMemoryStore", limit: int) -> list[dict[str, Any]]:
        """Return up to ``limit`` tasks ready for dispatch."""
        ...


class PriorityScheduler:
    """Default scheduler: highest-priority due tasks first."""

    def select(self, store: "InMemoryStore", limit: int) -> list[dict[str, Any]]:
        """Delegate to :meth:`InMemoryStore.due_tasks`."""
        return store.due_tasks(limit)


# ── store ─────────────────────────────────────────────────────────────────────


class InMemoryStore:
    """
    Pure in-memory task store for the NNG hub.

    All methods are synchronous and safe to call from a single asyncio event
    loop — asyncio's cooperative scheduling makes them effectively atomic (no
    ``await`` between reads and writes).

    State is lost when the process exits.  For persistent task queues use a
    dedicated result backend; the NNG broker is designed for low-latency
    in-process delivery, not durable storage.
    """

    def __init__(self, config: StoreConfig) -> None:
        """Initialise an empty store with the given configuration."""
        self.config = config
        self._tasks: dict[str, _Task] = {}
        self._workers: dict[str, _Worker] = {}

    # ── helpers ───────────────────────────────────────────────────────────────

    def _backoff(self, attempts: int) -> float:
        return min(
            self.config.backoff_cap,
            self.config.delivery_backoff * (2 ** max(0, attempts - 1)),
        )

    def _requeue_or_fail(self, task: _Task, worker_id: str, error: str) -> bool:
        now = time.time()
        # Hub-internal delivery cap.  User-level retries are handled by
        # retry middlewares, which re-kick the task with updated labels.
        if task.attempts > self.config.max_delivery_attempts:
            task.state = "failed"
        else:
            task.state = "ready"
            task.next_run_at = now + self._backoff(task.attempts)
        task.last_error = error
        task.lease_id = None
        task.leased_worker_id = None
        task.lease_until = None
        task.updated_at = now
        worker = self._workers.get(worker_id)
        if worker is not None:
            worker.inflight = max(0, worker.inflight - 1)
        return True

    # ── task lifecycle ────────────────────────────────────────────────────────

    def pending_count(self) -> int:
        """Return the count of ready and leased tasks."""
        return sum(1 for t in self._tasks.values() if t.state in ("ready", "leased"))

    def submit(self, envelope: TaskEnvelope) -> None:
        """
        Accept a new task into the store.

        :param envelope: task envelope to store.
        :raises QueueFullError: when ``max_pending`` is reached.
        """
        if self.pending_count() >= self.config.max_pending:
            raise QueueFullError("Task queue is full.")
        now = time.time()
        self._tasks[envelope.task_id] = _Task(
            task_id=envelope.task_id,
            task_name=envelope.task_name,
            payload=envelope.payload,
            labels=envelope.labels,
            state="ready",
            priority=envelope.priority,
            created_at=envelope.created_at or now,
            updated_at=now,
            next_run_at=now,
        )

    def due_tasks(self, limit: int = 50) -> list[dict[str, Any]]:
        """
        Return ready tasks whose ``next_run_at`` is in the past.

        Results are ordered by priority (descending) then creation time.

        :param limit: maximum number of rows to return.
        :return: list of task dicts.
        """
        now = time.time()
        ready = [
            t for t in self._tasks.values()
            if t.state == "ready" and t.next_run_at <= now
        ]
        ready.sort(key=lambda t: (-t.priority, t.created_at))
        return [t.as_dict() for t in ready[:limit]]

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
        :return: True on success; False if the task is not in 'ready' state.
        """
        task = self._tasks.get(task_id)
        if task is None or task.state != "ready":
            return False
        now = time.time()
        task.state = "leased"
        task.leased_worker_id = worker_id
        task.lease_id = lease_id
        task.lease_until = lease_until
        task.attempts += 1
        task.updated_at = now
        worker = self._workers.get(worker_id)
        if worker is not None:
            worker.inflight += 1
        return True

    def ack(self, task_id: str, worker_id: str, lease_id: str) -> bool:
        """
        Mark a task as successfully completed.

        Late or duplicate acks (mismatched ``lease_id`` or state ≠ 'leased')
        are silently rejected.

        :param task_id: task being acknowledged.
        :param worker_id: worker sending the ack.
        :param lease_id: lease token issued at dispatch.
        :return: True if the ack was accepted.
        """
        task = self._tasks.get(task_id)
        if task is None or task.state != "leased":
            return False
        if task.lease_id != lease_id or task.leased_worker_id != worker_id:
            return False
        now = time.time()
        task.state = "done"
        task.updated_at = now
        task.lease_id = None
        task.leased_worker_id = None
        task.lease_until = None
        worker = self._workers.get(worker_id)
        if worker is not None:
            worker.inflight = max(0, worker.inflight - 1)
        return True

    def nack(
        self, task_id: str, worker_id: str, lease_id: str, error: str
    ) -> bool:
        """
        Explicitly fail a task, triggering retry or permanent failure.

        :param task_id: task being nacked.
        :param worker_id: worker sending the nack.
        :param lease_id: lease token issued at dispatch.
        :param error: human-readable failure reason.
        :return: True if the nack was accepted.
        """
        task = self._tasks.get(task_id)
        if (
            task is None
            or task.state != "leased"
            or task.lease_id != lease_id
            or task.leased_worker_id != worker_id
        ):
            return False
        return self._requeue_or_fail(task, worker_id, error)

    # ── reaper / recovery ─────────────────────────────────────────────────────

    def reap_expired_leases(self) -> int:
        """
        Requeue or permanently fail tasks whose lease deadline has passed.

        :return: number of tasks reaped.
        """
        now = time.time()
        expired = [
            t for t in self._tasks.values()
            if t.state == "leased"
            and t.lease_until is not None
            and t.lease_until < now
        ]
        for task in expired:
            self._requeue_or_fail(task, task.leased_worker_id or "", "lease expired")
        return len(expired)

    def recover_dead_workers(self, heartbeat_timeout: float) -> int:
        """
        Mark workers that missed their heartbeat deadline as dead and requeue their tasks.

        :param heartbeat_timeout: seconds of silence before a worker is dead.
        :return: number of tasks requeued.
        """
        cutoff = time.time() - heartbeat_timeout
        dead = [
            w for w in self._workers.values()
            if w.last_seen < cutoff and w.status != "dead"
        ]
        requeued = 0
        for worker in dead:
            worker.status = "dead"
            worker.draining = True
            leased = [
                t for t in self._tasks.values()
                if t.state == "leased" and t.leased_worker_id == worker.worker_id
            ]
            for task in leased:
                self._requeue_or_fail(task, worker.worker_id, "worker died")
                requeued += 1
        return requeued

    # ── worker lifecycle ──────────────────────────────────────────────────────

    def register_worker(self, worker: WorkerState) -> None:
        """
        Upsert a worker record, resetting drain state on re-registration.

        :param worker: worker state snapshot from the registration message.
        """
        now = time.time()
        existing = self._workers.get(worker.worker_id)
        if existing is not None:
            existing.task_addr = worker.task_addr
            existing.capacity = worker.capacity
            existing.last_seen = now
            existing.heartbeat_interval = worker.heartbeat_interval
            existing.lease_timeout = worker.lease_timeout
            existing.draining = False
            existing.status = "listening"
            existing.version = worker.version
        else:
            self._workers[worker.worker_id] = _Worker(
                worker_id=worker.worker_id,
                task_addr=worker.task_addr,
                capacity=worker.capacity,
                inflight=0,
                last_seen=now,
                heartbeat_interval=worker.heartbeat_interval,
                lease_timeout=worker.lease_timeout,
                draining=False,
                status="listening",
                version=worker.version,
            )

    def heartbeat(self, worker_id: str) -> None:
        """
        Record a heartbeat, resetting the worker's last_seen timestamp.

        :param worker_id: ID of the worker sending the heartbeat.
        """
        worker = self._workers.get(worker_id)
        if worker is not None:
            worker.last_seen = time.time()
            worker.status = "listening"

    def unregister_worker(self, worker_id: str) -> None:
        """
        Remove a worker from the registry (graceful shutdown path).

        :param worker_id: ID of the worker unregistering.
        """
        self._workers.pop(worker_id, None)

    def mark_draining(self, worker_id: str) -> None:
        """
        Mark a worker as draining so the hub stops dispatching new tasks to it.

        :param worker_id: ID of the worker entering drain mode.
        """
        worker = self._workers.get(worker_id)
        if worker is not None:
            worker.draining = True
            worker.status = "draining"

    # ── routing ───────────────────────────────────────────────────────────────

    def choose_worker(
        self,
        policy: "RoutingPolicy | str" = "least_loaded",
        *,
        heartbeat_timeout: float = 15.0,
        task: "TaskContext | None" = None,
    ) -> dict[str, Any] | None:
        """
        Select the best available worker using a routing policy.

        Accepts a :class:`RoutingPolicy` instance or a string name
        (``'least_loaded'``, ``'p2c'``, ``'round_robin'``, ``'affinity'``).

        Context-aware policies (e.g. :class:`AffinityPolicy`) receive the
        optional ``task`` argument when they declare it in their ``choose``
        signature.

        :param policy: routing policy or name.
        :param heartbeat_timeout: seconds before a worker is considered stale.
        :param task: optional task context for context-aware policies.
        :return: chosen worker dict, or None if no worker has capacity.
        """
        cutoff = time.time() - heartbeat_timeout
        available = [
            w for w in self._workers.values()
            if w.status in ("starting", "listening")
            and not w.draining
            and w.last_seen >= cutoff
            and w.inflight < w.capacity
        ]
        if not available:
            return None
        # Stable sort so RoundRobin cycles in a predictable, deterministic order.
        views = sorted(
            [WorkerView(w.worker_id, w.inflight, w.capacity) for w in available],
            key=lambda v: v.worker_id,
        )
        routing = make_routing_policy(policy)
        chosen = _choose_with_context(routing, views, task)
        if chosen is None:
            return None
        worker = self._workers.get(chosen.worker_id)
        return worker.as_dict() if worker is not None else None

    # ── observability ─────────────────────────────────────────────────────────

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        """
        Fetch task status by ID (no raw bytes in result).

        :param task_id: ID of the task to look up.
        :return: status dict or None if not found.
        """
        task = self._tasks.get(task_id)
        return task.as_status_dict() if task is not None else None

    def list_workers(self) -> list[dict[str, Any]]:
        """Return all registered workers ordered by most-recently-seen."""
        return [
            w.as_dict()
            for w in sorted(
                self._workers.values(), key=lambda w: w.last_seen, reverse=True
            )
        ]

    def stats(self) -> dict[str, int]:
        """Return a summary dict with task state counts and active worker count."""
        counts: dict[str, int] = {}
        for t in self._tasks.values():
            counts[t.state] = counts.get(t.state, 0) + 1
        active = sum(
            1 for w in self._workers.values()
            if w.status in ("starting", "listening") and not w.draining
        )
        return {
            "ready": counts.get("ready", 0),
            "leased": counts.get("leased", 0),
            "done": counts.get("done", 0),
            "failed": counts.get("failed", 0),
            "active_workers": active,
        }
