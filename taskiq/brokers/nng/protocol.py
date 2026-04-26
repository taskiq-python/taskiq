"""Wire protocol types for the NNG broker."""
from __future__ import annotations

import base64
import enum
import json
from dataclasses import asdict, dataclass, field
from typing import Any


class _StrValue(str, enum.Enum):
    """Base for string enums whose str() returns the plain value (Python 3.10+)."""

    def __str__(self) -> str:
        return self.value


class MessageKind(_StrValue):
    """Kinds of control-plane messages sent between broker/client and hub."""

    SUBMIT = "submit"
    REGISTER = "register"
    HEARTBEAT = "heartbeat"
    UNREGISTER = "unregister"
    DRAIN = "drain"
    ACK = "ack"
    NACK = "nack"
    STATUS = "status"
    STATS = "stats"
    PING = "ping"


class TaskState(_StrValue):
    """Lifecycle state of a task in the hub store."""

    READY = "ready"
    LEASED = "leased"
    DONE = "done"
    FAILED = "failed"


class WorkerStatus(_StrValue):
    """Lifecycle status of a registered worker."""

    STARTING = "starting"
    LISTENING = "listening"
    DRAINING = "draining"
    OFFLINE = "offline"
    DEAD = "dead"


@dataclass
class TaskEnvelope:
    """
    Task payload sent from hub to worker over the data plane.

    ``lease_id`` is the UUID assigned by the hub at dispatch time.
    Workers must echo it back in the ACK so the hub can validate
    that the ack is not stale (e.g. after lease expiry and requeue).
    """

    task_id: str
    task_name: str
    payload_b64: str
    labels: dict[str, Any] = field(default_factory=dict)
    lease_id: str = ""
    attempts: int = 0
    max_retries: int = 0
    retry_backoff: float = 1.0
    retry_jitter: float = 0.0
    priority: int = 0
    created_at: float = 0.0

    @property
    def payload(self) -> bytes:
        """Decode the base-64 task payload."""
        return base64.b64decode(self.payload_b64.encode("ascii"))

    @classmethod
    def from_bytes(cls, raw: bytes) -> TaskEnvelope:
        """Deserialise from JSON bytes."""
        return cls(**json.loads(raw.decode("utf-8")))

    def to_bytes(self) -> bytes:
        """Serialise to JSON bytes."""
        return json.dumps(
            asdict(self), separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")


@dataclass
class ControlMessage:
    """Request sent over the control plane (Req0 → Rep0)."""

    kind: str
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_bytes(cls, raw: bytes) -> ControlMessage:
        """Deserialise from JSON bytes."""
        data = json.loads(raw.decode("utf-8"))
        return cls(kind=data["kind"], payload=data.get("payload", {}))

    def to_bytes(self) -> bytes:
        """Serialise to JSON bytes."""
        return json.dumps(
            {"kind": self.kind, "payload": self.payload},
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")


@dataclass
class ControlResponse:
    """Response sent back over the control plane (Rep0 → Req0)."""

    ok: bool
    payload: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    @classmethod
    def from_bytes(cls, raw: bytes) -> ControlResponse:
        """Deserialise from JSON bytes."""
        data = json.loads(raw.decode("utf-8"))
        return cls(
            ok=data["ok"],
            payload=data.get("payload", {}),
            error=data.get("error"),
        )

    def to_bytes(self) -> bytes:
        """Serialise to JSON bytes."""
        return json.dumps(
            {"ok": self.ok, "payload": self.payload, "error": self.error},
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")


@dataclass
class WorkerState:
    """Snapshot of a worker's identity and capacity at registration time."""

    worker_id: str
    task_addr: str
    capacity: int
    inflight: int = 0
    last_seen: float = 0.0
    heartbeat_interval: float = 5.0
    lease_timeout: float = 15.0
    draining: bool = False
    status: WorkerStatus = WorkerStatus.STARTING
    version: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict, serialising the status enum to its string value."""
        d = asdict(self)
        d["status"] = str(self.status)
        return d
