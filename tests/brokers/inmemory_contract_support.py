import asyncio
from typing import Any

from taskiq import InMemoryBroker, TaskiqMessage, TaskiqResult
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.brokers.inmemory_broker import InmemoryResultBackend


class LifecycleError(Exception):
    """Marker exception for lifecycle fault tests."""


class PostSendError(Exception):
    """Marker exception for post-send compatibility tests."""


class RecordingLifecycleMiddleware(TaskiqMiddleware):
    """Record middleware lifecycle calls and optional shutdown failure."""

    def __init__(
        self,
        events: list[str],
        shutdown_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self.events = events
        self.shutdown_error = shutdown_error

    async def startup(self) -> None:
        self.events.append("middleware.startup")

    async def shutdown(self) -> None:
        self.events.append("middleware.shutdown")
        if self.shutdown_error is not None:
            raise self.shutdown_error


class RecordingResultBackend(InmemoryResultBackend[Any]):
    """Record result backend lifecycle calls and optional failures."""

    def __init__(
        self,
        events: list[str],
        startup_error: Exception | None = None,
        shutdown_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self.events = events
        self.startup_error = startup_error
        self.shutdown_error = shutdown_error

    async def startup(self) -> None:
        self.events.append("backend.startup")
        if self.startup_error is not None:
            raise self.startup_error

    async def shutdown(self) -> None:
        self.events.append("backend.shutdown")
        if self.shutdown_error is not None:
            raise self.shutdown_error


class FailingExecutionMiddleware(TaskiqMiddleware):
    """Raise before execution to expose background callback failures."""

    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error

    def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        raise self.error


class BlockingPostSendMiddleware(TaskiqMiddleware):
    """Pause post-send to prove execution cannot overlap it."""

    def __init__(
        self,
        events: list[str],
        post_send_started: asyncio.Event,
        release_post_send: asyncio.Event,
    ) -> None:
        super().__init__()
        self.events = events
        self.post_send_started = post_send_started
        self.release_post_send = release_post_send

    def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        self.events.append("pre_send")
        return message

    async def post_send(self, message: TaskiqMessage) -> None:
        self.events.append("post_send.started")
        self.post_send_started.set()
        await self.release_post_send.wait()
        self.events.append("post_send.finished")

    def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        self.events.append("pre_execute")
        return message

    def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        self.events.append("post_execute")


class CoordinatedPostSendMiddleware(TaskiqMiddleware):
    """Hold two post-send hooks at different deterministic boundaries."""

    def __init__(
        self,
        first_task_name: str,
        second_task_name: str,
        first_post_send_started: asyncio.Event,
        second_post_send_started: asyncio.Event,
        release_second_post_send: asyncio.Event,
    ) -> None:
        super().__init__()
        self.first_task_name = first_task_name
        self.second_task_name = second_task_name
        self.first_post_send_started = first_post_send_started
        self.second_post_send_started = second_post_send_started
        self.release_second_post_send = release_second_post_send

    async def post_send(self, message: TaskiqMessage) -> None:
        if message.task_name == self.first_task_name:
            self.first_post_send_started.set()
            await self.second_post_send_started.wait()
            return

        if message.task_name == self.second_task_name:
            self.second_post_send_started.set()
            await self.release_second_post_send.wait()


class FailingPostSendMiddleware(TaskiqMiddleware):
    """Fail after transport acceptance."""

    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error

    def post_send(self, message: TaskiqMessage) -> None:
        raise self.error


class ReentrantDrainMiddleware(TaskiqMiddleware):
    """Attempt a broker drain from the active send lifecycle."""

    def __init__(self, operation: str) -> None:
        super().__init__()
        self.operation = operation

    async def post_send(self, message: TaskiqMessage) -> None:
        await getattr(self.broker, self.operation)()


class DrainSignallingInMemoryBroker(InMemoryBroker):
    """Expose drain attempts for deterministic overlap tests."""

    def __init__(
        self,
        drain_started: asyncio.Event,
        drain_restarted: asyncio.Event | None = None,
        *,
        await_inplace: bool = False,
    ) -> None:
        super().__init__(await_inplace=await_inplace)
        self.drain_started = drain_started
        self.drain_restarted = drain_restarted
        self.drain_calls = 0

    async def wait_all(self) -> None:
        self.drain_calls += 1
        if self.drain_calls == 1:
            self.drain_started.set()
        elif self.drain_restarted is not None:
            self.drain_restarted.set()
        await super().wait_all()
