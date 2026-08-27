import asyncio
from typing import Any, cast

from taskiq import InMemoryBroker, TaskiqMessage, TaskiqResult
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.brokers.inmemory_broker import InmemoryResultBackend
from taskiq.message import BrokerMessage


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

    def __init__(self, error: BaseException) -> None:
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


class BlockingPreSendMiddleware(TaskiqMiddleware):
    """Pause pre-send and expose subsequent resource shutdown."""

    def __init__(
        self,
        pre_send_started: asyncio.Event,
        release_pre_send: asyncio.Event,
        shutdown_started: asyncio.Event,
    ) -> None:
        super().__init__()
        self.pre_send_started = pre_send_started
        self.release_pre_send = release_pre_send
        self.shutdown_started = shutdown_started
        self.pre_send_calls = 0

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        self.pre_send_calls += 1
        self.pre_send_started.set()
        await self.release_pre_send.wait()
        return message

    async def shutdown(self) -> None:
        self.shutdown_started.set()


class BlockingShutdownMiddleware(TaskiqMiddleware):
    """Block middleware shutdown until the test releases it."""

    def __init__(
        self,
        shutdown_started: asyncio.Event,
        release_shutdown: asyncio.Event,
        shutdown_finished: asyncio.Event,
    ) -> None:
        super().__init__()
        self.shutdown_started = shutdown_started
        self.release_shutdown = release_shutdown
        self.shutdown_finished = shutdown_finished
        self.shutdown_calls = 0

    async def shutdown(self) -> None:
        self.shutdown_calls += 1
        self.shutdown_started.set()
        await self.release_shutdown.wait()
        self.shutdown_finished.set()


class BlockingShutdownResultBackend(InmemoryResultBackend[Any]):
    """Block backend shutdown until the test releases it."""

    def __init__(
        self,
        shutdown_started: asyncio.Event,
        release_shutdown: asyncio.Event,
        shutdown_finished: asyncio.Event,
    ) -> None:
        super().__init__()
        self.shutdown_started = shutdown_started
        self.release_shutdown = release_shutdown
        self.shutdown_finished = shutdown_finished

    async def shutdown(self) -> None:
        self.shutdown_started.set()
        await self.release_shutdown.wait()
        self.shutdown_finished.set()


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


class ChildReentrantDrainMiddleware(TaskiqMiddleware):
    """Attempt a broker drain from a child of the active send lifecycle."""

    def __init__(self, operation: str) -> None:
        super().__init__()
        self.operation = operation

    async def post_send(self, message: TaskiqMessage) -> None:
        drain = asyncio.create_task(getattr(self.broker, self.operation)())
        await drain


class DeferredChildDrainMiddleware(TaskiqMiddleware):
    """Drain from a child after its originating send lifecycle has ended."""

    def __init__(self, release: asyncio.Event) -> None:
        super().__init__()
        self.release = release
        self.drain_task: asyncio.Task[BaseException | None] | None = None

    async def post_send(self, message: TaskiqMessage) -> None:
        self.drain_task = asyncio.create_task(self._drain_after_release())

    async def _drain_after_release(self) -> BaseException | None:
        await self.release.wait()
        try:
            await cast(InMemoryBroker, self.broker).wait_all()
        except BaseException as exc:
            return exc
        return None


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


class RecordingKickInMemoryBroker(InMemoryBroker):
    """Record calls through the public kick extension point."""

    def __init__(self, *, await_inplace: bool) -> None:
        super().__init__(await_inplace=await_inplace)
        self.kick_calls = 0

    async def kick(self, message: BrokerMessage) -> None:
        self.kick_calls += 1
        await super().kick(message)


class BlockingAfterAcceptKickInMemoryBroker(InMemoryBroker):
    """Block a public kick override after its base send was accepted."""

    def __init__(self, *, await_inplace: bool) -> None:
        super().__init__(await_inplace=await_inplace)
        self.accepted = asyncio.Event()
        self.release = asyncio.Event()

    async def kick(self, message: BrokerMessage) -> None:
        await super().kick(message)
        self.accepted.set()
        await self.release.wait()


class FailingAfterAcceptKickInMemoryBroker(InMemoryBroker):
    """Fail a public kick override after its base send was accepted."""

    def __init__(
        self,
        kick_error: Exception,
        *,
        await_inplace: bool,
    ) -> None:
        super().__init__(await_inplace=await_inplace)
        self.kick_error = kick_error

    async def kick(self, message: BrokerMessage) -> None:
        await super().kick(message)
        raise self.kick_error


class TransformingKickInMemoryBroker(InMemoryBroker):
    """Replace the locally accepted message through the public kick seam."""

    def __init__(
        self,
        replacement_task_name: str,
        *,
        await_inplace: bool,
    ) -> None:
        super().__init__(await_inplace=await_inplace)
        self.replacement_task_name = replacement_task_name

    async def kick(self, message: BrokerMessage) -> None:
        accepted_message = self.formatter.loads(message.message)
        accepted_message.task_name = self.replacement_task_name
        await super().kick(self.formatter.dumps(accepted_message))


class RejectingKickInMemoryBroker(InMemoryBroker):
    """Reject one public send before post-send middleware can observe it."""

    def __init__(
        self,
        kick_error: Exception,
        *,
        await_inplace: bool,
    ) -> None:
        super().__init__(await_inplace=await_inplace)
        self.kick_error = kick_error
        self.kick_calls = 0

    async def kick(self, message: BrokerMessage) -> None:
        self.kick_calls += 1
        raise self.kick_error


class ReplacingKickInMemoryBroker(InMemoryBroker):
    """Replace local dispatch while preserving the public send pipeline."""

    def __init__(self, *, await_inplace: bool) -> None:
        super().__init__(await_inplace=await_inplace)
        self.kick_calls = 0

    async def kick(self, message: BrokerMessage) -> None:
        self.kick_calls += 1
