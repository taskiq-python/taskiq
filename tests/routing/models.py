from collections.abc import AsyncGenerator, Mapping
from dataclasses import dataclass
from typing import Any

from taskiq import AsyncTaskiqDecoratedTask, FlowProtocol, TaskiqRouter
from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.result import TaskiqResult


@dataclass(frozen=True, slots=True)
class BrokerQueue:
    """Broker-specific flow used to prove protocol-based routing."""

    name: str
    durable: bool = True

    def broker_options(self) -> Mapping[str, object]:
        """Return options for the target broker."""
        return {
            "durable": self.durable,
        }


class TracingTask(AsyncTaskiqDecoratedTask[Any, Any]):
    """Custom task class used to prove task_builder base_cls binding."""

    def tracing_name(self) -> str:
        """Return a trace-friendly task name."""
        return self.task_name


class CountingRouter(TaskiqRouter):
    """Router that counts task registration calls."""

    def __init__(self) -> None:
        self.register_task_calls = 0
        super().__init__()

    def register_task(
        self,
        task: Any,
        broker: AsyncBroker | None = None,
        flow: FlowProtocol | None = None,
    ) -> Any:
        """Count registration calls and delegate to the router."""
        self.register_task_calls += 1
        return super().register_task(task, broker=broker, flow=flow)


class OldStyleRecordingBroker(AsyncBroker):
    """Broker that only implements the old required kick/listen API."""

    def __init__(
        self,
        *,
        router: TaskiqRouter | None = None,
        broker_name: str | None = None,
    ) -> None:
        self.sent: list[BrokerMessage] = []
        super().__init__(router=router, broker_name=broker_name)

    async def kick(self, message: BrokerMessage) -> None:
        """Record old-style send."""
        self.sent.append(message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """Old-style recording broker does not listen in tests."""
        if False:
            yield b""


class RecordingResultBackend(AsyncResultBackend[Any]):
    """Result backend marker that records stored results."""

    def __init__(self) -> None:
        self.results: dict[str, TaskiqResult[Any]] = {}

    async def set_result(self, task_id: str, result: TaskiqResult[Any]) -> None:
        """Store result by task id."""
        self.results[task_id] = result

    async def is_result_ready(self, task_id: str) -> bool:
        """Return whether result exists."""
        return task_id in self.results

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[Any]:
        """Return stored result."""
        return self.results[task_id]


class RecordingMiddleware(TaskiqMiddleware):
    """Middleware that records client-side send hooks."""

    def __init__(
        self,
        name: str,
        events: list[tuple[str, str, str]],
    ) -> None:
        super().__init__()
        self.name = name
        self.events = events

    def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        """Record pre-send call."""
        self.events.append((self.name, "pre_send", message.task_name))
        return message

    def post_send(self, message: TaskiqMessage) -> None:
        """Record post-send call."""
        self.events.append((self.name, "post_send", message.task_name))
