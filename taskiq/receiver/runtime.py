"""Worker-process orchestration for one or several broker listeners."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Mapping, Sequence
from concurrent.futures import Executor
from dataclasses import dataclass, field
from logging import getLogger
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast

from taskiq.abc.broker import AsyncBroker
from taskiq.acks import AcknowledgeType

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.receiver.receiver import Receiver

logger = getLogger("taskiq.worker.runtime")


async def _call_broker_shutdown(broker: AsyncBroker) -> object | None:
    """Call shutdown while tolerating legacy diagnostic return values."""
    # Third-party brokers historically returned diagnostics even though the
    # public AsyncBroker contract is annotated as returning None.
    return await broker.shutdown()  # type: ignore[func-returns-value]


def _consume_late_shutdown_result(
    shutdown_task: asyncio.Task[object | None],
) -> None:
    """Retrieve the outcome of a shutdown task detached after its deadline."""
    try:
        shutdown_task.result()
    except asyncio.CancelledError:
        pass
    except BaseException:
        logger.exception(
            "Broker shutdown task %s failed after its cleanup deadline.",
            shutdown_task.get_name(),
        )


async def _shutdown_broker(
    broker: AsyncBroker,
    timeout: float,
) -> asyncio.CancelledError | None:
    """Shut down one broker without preventing later resource cleanup."""
    logger.info("Shutting down broker %s.", broker.broker_name)
    shutdown_task = asyncio.create_task(
        _call_broker_shutdown(broker),
        name=f"taskiq-shutdown-{broker.broker_name}",
    )
    try:
        done, _ = await asyncio.wait(
            (shutdown_task,),
            timeout=timeout,
        )
    except asyncio.CancelledError as exc:
        shutdown_task.cancel()
        shutdown_task.add_done_callback(_consume_late_shutdown_result)
        logger.warning(
            "Broker %s shutdown was interrupted by cancellation.",
            broker.broker_name,
        )
        return exc

    if shutdown_task not in done:
        shutdown_task.cancel()
        shutdown_task.add_done_callback(_consume_late_shutdown_result)
        logger.warning(
            "Broker %s did not shut down in %s seconds.",
            broker.broker_name,
            timeout,
        )
        return None

    try:
        result = shutdown_task.result()
    except asyncio.CancelledError as exc:
        logger.warning(
            "Broker %s failed during shutdown: %s",
            broker.broker_name,
            type(exc).__name__,
            exc_info=True,
        )
        return exc
    except Exception as exc:
        logger.warning(
            "Broker %s failed during shutdown: %s",
            broker.broker_name,
            type(exc).__name__,
            exc_info=True,
        )
        return None

    if result is not None:
        logger.info(
            "Broker %s returned a diagnostic value during shutdown.",
            broker.broker_name,
        )
    return None


class WorkerRuntimeConfigurationError(ValueError):
    """Raised when a multi-broker worker configuration is inconsistent."""


@dataclass(frozen=True, slots=True)
class BrokerSelection:
    """Normalized listener selection resolved from the CLI import target."""

    listeners: tuple[AsyncBroker, ...]
    explicit_group: bool

    def mark_listener_brokers(self) -> None:
        """Set worker mode before task modules and adapter resources are loaded."""
        if not self.explicit_group:
            self.listeners[0].is_worker_process = True
            return

        listener_ids = {id(broker) for broker in self.listeners}
        for broker in self.listeners[0].router.brokers.values():
            broker.is_worker_process = id(broker) in listener_ids

    def managed_brokers(self) -> tuple[AsyncBroker, ...]:
        """Return listener brokers plus outbound-only lifecycle participants."""
        if not self.explicit_group:
            return self.listeners

        listener_ids = {id(broker) for broker in self.listeners}
        outbound_brokers = tuple(
            broker
            for broker in self.listeners[0].router.brokers.values()
            if id(broker) not in listener_ids
        )
        for broker in outbound_brokers:
            broker.is_worker_process = False
        return (*self.listeners, *outbound_brokers)


def normalize_broker_source(source: object) -> BrokerSelection:
    """Normalize one broker or an explicit listener sequence."""
    listeners, explicit_group = _coerce_listener_tuple(source)
    _validate_listener_tuple(listeners)
    return BrokerSelection(listeners=listeners, explicit_group=explicit_group)


def _coerce_listener_tuple(
    source: object,
) -> tuple[tuple[AsyncBroker, ...], bool]:
    """Coerce a supported import target into a listener tuple."""
    if isinstance(source, AsyncBroker):
        return (source,), False
    if isinstance(source, Sequence) and not isinstance(
        source,
        (str, bytes, bytearray),
    ):
        return cast(tuple[AsyncBroker, ...], tuple(source)), True
    raise WorkerRuntimeConfigurationError(
        "Worker target must be an AsyncBroker or a sequence of AsyncBroker "
        "instances.",
    )


def _validate_listener_tuple(listeners: tuple[AsyncBroker, ...]) -> None:
    """Validate listener identity and shared Router ownership."""
    if not listeners:
        raise WorkerRuntimeConfigurationError("Worker broker sequence cannot be empty.")

    for index, broker in enumerate(listeners):
        if not isinstance(broker, AsyncBroker):
            raise WorkerRuntimeConfigurationError(
                f"Worker broker at index {index} is not an AsyncBroker instance.",
            )

    if len({id(broker) for broker in listeners}) != len(listeners):
        raise WorkerRuntimeConfigurationError(
            "Worker broker sequence contains the same broker more than once.",
        )

    router = listeners[0].router
    if any(broker.router is not router for broker in listeners[1:]):
        raise WorkerRuntimeConfigurationError(
            "Multi-broker listeners must use the same router instance.",
        )

    broker_names: set[str] = set()
    for broker in listeners:
        if broker.broker_name in broker_names:
            raise WorkerRuntimeConfigurationError(
                f"Worker broker name {broker.broker_name!r} is not unique.",
            )
        broker_names.add(broker.broker_name)
        if router.brokers.get(broker.broker_name) is not broker:
            raise WorkerRuntimeConfigurationError(
                f"Worker broker {broker.broker_name!r} is not owned by its router.",
            )


@dataclass(frozen=True, slots=True)
class WorkerRuntimeOptions:
    """Receiver and lifecycle settings shared by a worker process."""

    validate_params: bool
    max_async_tasks: int | None
    max_async_tasks_jitter: int
    max_prefetch: int
    propagate_exceptions: bool
    ack_type: AcknowledgeType | None
    max_tasks_to_execute: int | None
    wait_tasks_timeout: float | None
    shutdown_timeout: float
    receiver_kwargs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.max_prefetch < 0:
            raise WorkerRuntimeConfigurationError("max_prefetch cannot be negative.")
        if self.max_async_tasks_jitter < 0:
            raise WorkerRuntimeConfigurationError(
                "max_async_tasks_jitter cannot be negative.",
            )
        if self.shutdown_timeout < 0:
            raise WorkerRuntimeConfigurationError(
                "shutdown_timeout cannot be negative.",
            )
        if self.wait_tasks_timeout is not None and self.wait_tasks_timeout < 0:
            raise WorkerRuntimeConfigurationError(
                "wait_tasks_timeout cannot be negative.",
            )
        object.__setattr__(
            self,
            "receiver_kwargs",
            MappingProxyType(dict(self.receiver_kwargs)),
        )


class ReceiverRuntimeState:
    """Process-wide budgets shared by child Receiver instances."""

    def __init__(
        self,
        *,
        finish_event: asyncio.Event,
        max_async_tasks: int | None,
        max_async_tasks_jitter: int,
        max_prefetch: int,
        max_tasks_to_execute: int | None,
    ) -> None:
        actual_limit = max_async_tasks
        if actual_limit is not None and actual_limit > 0:
            if max_async_tasks_jitter > 0:
                actual_limit += random.randint(  # noqa: S311
                    0,
                    max_async_tasks_jitter,
                )
            self.execution_semaphore: asyncio.Semaphore | None = asyncio.Semaphore(
                actual_limit,
            )
        else:
            self.execution_semaphore = None

        self.prefetch_semaphore = asyncio.Semaphore(max_prefetch + 1)
        self.finish_event = finish_event
        self.max_tasks_to_execute = max_tasks_to_execute
        self.fetched_tasks = 0

    def record_fetched_task(self) -> None:
        """Count one fetched delivery and stop the process at its shared limit."""
        self.fetched_tasks += 1
        if (
            self.max_tasks_to_execute
            and self.fetched_tasks >= self.max_tasks_to_execute
        ):
            self.finish_event.set()


class WorkerRuntime:
    """Own broker lifecycle and concurrent Receiver listeners in one process."""

    def __init__(
        self,
        *,
        selection: BrokerSelection,
        receiver_type: type[Receiver],
        executor: Executor,
        options: WorkerRuntimeOptions,
    ) -> None:
        selection.mark_listener_brokers()
        self.selection = selection
        self.managed_brokers = selection.managed_brokers()
        self.receiver_type = receiver_type
        self.executor = executor
        self.options = options
        self._validate_runtime_configuration()

    async def run(self, finish_event: asyncio.Event) -> None:
        """Start resources, run every listener and clean up deterministically."""
        primary_error: BaseException | None = None
        started_brokers: list[AsyncBroker] = []

        try:
            runtime_state = ReceiverRuntimeState(
                finish_event=finish_event,
                max_async_tasks=self.options.max_async_tasks,
                max_async_tasks_jitter=self.options.max_async_tasks_jitter,
                max_prefetch=self.options.max_prefetch,
                max_tasks_to_execute=self.options.max_tasks_to_execute,
            )
            receivers = self._build_receivers(runtime_state)

            for broker in self.managed_brokers:
                started_brokers.append(broker)
                logger.info("Starting broker %s.", broker.broker_name)
                await broker.startup()

            await self._run_listeners(receivers, finish_event)
        except BaseException as exc:
            primary_error = exc
        finally:
            finish_event.set()
            cleanup_cancellation = await self._shutdown_brokers(started_brokers)
            if primary_error is None:
                primary_error = cleanup_cancellation

        if primary_error is not None:
            raise primary_error

    def _build_receivers(
        self,
        runtime_state: ReceiverRuntimeState,
    ) -> tuple[Receiver, ...]:
        receivers = []
        for broker in self.selection.listeners:
            receiver = self.receiver_type(
                broker=broker,
                executor=self.executor,
                run_startup=False,
                validate_params=self.options.validate_params,
                max_async_tasks=self.options.max_async_tasks,
                max_async_tasks_jitter=self.options.max_async_tasks_jitter,
                max_prefetch=self.options.max_prefetch,
                propagate_exceptions=self.options.propagate_exceptions,
                ack_type=self.options.ack_type,
                max_tasks_to_execute=self.options.max_tasks_to_execute,
                wait_tasks_timeout=self.options.wait_tasks_timeout,
                **self.options.receiver_kwargs,
            )
            attached_state = receiver.attach_runtime_state(runtime_state)
            if (
                attached_state is not runtime_state
                or not receiver.is_runtime_state_attached(runtime_state)
            ):
                raise WorkerRuntimeConfigurationError(
                    "Custom Receiver.attach_runtime_state() must preserve the "
                    "process-wide worker state.",
                )
            receivers.append(receiver)
        return tuple(receivers)

    async def _run_listeners(
        self,
        receivers: tuple[Receiver, ...],
        finish_event: asyncio.Event,
    ) -> None:
        listener_tasks = tuple(
            asyncio.create_task(
                receiver.listen(finish_event),
                name=f"taskiq-listener-{receiver.broker.broker_name}",
            )
            for receiver in receivers
        )
        finish_waiter = asyncio.create_task(
            finish_event.wait(),
            name="taskiq-worker-finish",
        )
        primary_error: BaseException | None = None

        try:
            first_done, _ = await asyncio.wait(
                (*listener_tasks, finish_waiter),
                return_when=asyncio.FIRST_COMPLETED,
            )
            completed_listeners = {
                listener_task
                for listener_task in listener_tasks
                if listener_task in first_done
            }
            primary_error = self._find_listener_error(
                listener_tasks,
                completed_listeners,
            )
        except BaseException as exc:
            primary_error = exc
        finally:
            finish_event.set()
            if not finish_waiter.done():
                finish_waiter.cancel()
            await asyncio.gather(finish_waiter, return_exceptions=True)
            results, hard_cancelled = await self._settle_listener_tasks(
                listener_tasks,
                receivers,
            )

        if primary_error is None:
            for listener_task, result in zip(listener_tasks, results, strict=True):
                if listener_task in hard_cancelled and isinstance(
                    result,
                    asyncio.CancelledError,
                ):
                    continue
                if isinstance(result, BaseException):
                    primary_error = result
                    break

        if primary_error is not None:
            raise primary_error

    @classmethod
    def _find_listener_error(
        cls,
        listener_tasks: tuple[asyncio.Task[None], ...],
        completed_tasks: set[asyncio.Task[None]],
    ) -> BaseException | None:
        """Select the first failed listener in deterministic broker order."""
        for listener_task in listener_tasks:
            if listener_task not in completed_tasks:
                continue
            result = cls._get_listener_result(listener_task)
            if isinstance(result, BaseException):
                return result
        return None

    async def _settle_listener_tasks(
        self,
        listener_tasks: tuple[asyncio.Task[None], ...],
        receivers: tuple[Receiver, ...],
    ) -> tuple[list[None | BaseException], set[asyncio.Task[None]]]:
        graceful_timeout = None
        if self.options.wait_tasks_timeout is not None:
            graceful_timeout = (
                self.options.wait_tasks_timeout + self.options.shutdown_timeout
            )
        _, pending = await asyncio.wait(
            listener_tasks,
            timeout=graceful_timeout,
        )
        hard_cancelled = set(pending)
        for listener_task, receiver in zip(listener_tasks, receivers, strict=True):
            if listener_task not in hard_cancelled:
                continue
            logger.warning(
                "Listener for broker %s did not stop in %s seconds; cancelling it.",
                receiver.broker.broker_name,
                graceful_timeout,
            )
            listener_task.cancel()

        if hard_cancelled:
            _, stubborn_tasks = await asyncio.wait(
                hard_cancelled,
                timeout=self.options.shutdown_timeout,
            )
            for listener_task, receiver in zip(
                listener_tasks,
                receivers,
                strict=True,
            ):
                if listener_task not in stubborn_tasks:
                    continue
                logger.error(
                    "Listener for broker %s ignored cancellation; continuing "
                    "broker cleanup.",
                    receiver.broker.broker_name,
                )
                listener_task.add_done_callback(self._consume_listener_result)

        results = [
            (
                self._get_listener_result(listener_task)
                if listener_task.done()
                else asyncio.CancelledError()
            )
            for listener_task in listener_tasks
        ]
        return results, hard_cancelled

    @staticmethod
    def _get_listener_result(
        listener_task: asyncio.Task[None],
    ) -> None | BaseException:
        """Return one listener outcome without re-raising it."""
        try:
            return listener_task.result()
        except BaseException as exc:
            return exc

    @staticmethod
    def _consume_listener_result(listener_task: asyncio.Task[None]) -> None:
        """Retrieve a late outcome from a cancellation-resistant listener."""
        try:
            listener_task.result()
        except asyncio.CancelledError:
            pass
        except BaseException:
            logger.exception(
                "Cancellation-resistant listener failed after runtime cleanup.",
            )

    async def _shutdown_brokers(
        self,
        started_brokers: list[AsyncBroker],
    ) -> asyncio.CancelledError | None:
        cleanup_cancellation: asyncio.CancelledError | None = None
        for broker in reversed(started_brokers):
            broker_cancellation = await _shutdown_broker(
                broker,
                self.options.shutdown_timeout,
            )
            if cleanup_cancellation is None:
                cleanup_cancellation = broker_cancellation
        return cleanup_cancellation

    def _validate_runtime_configuration(self) -> None:
        self._validate_unshared_resources()
        self._validate_task_lookup()

    def _validate_unshared_resources(self) -> None:
        middleware_owners: dict[int, str] = {}
        backend_owners: dict[int, str] = {}

        for broker in self.managed_brokers:
            for middleware in broker.middlewares:
                previous_owner = middleware_owners.setdefault(
                    id(middleware),
                    broker.broker_name,
                )
                if previous_owner != broker.broker_name:
                    raise WorkerRuntimeConfigurationError(
                        "One middleware instance cannot be lifecycle-owned by "
                        f"brokers {previous_owner!r} and {broker.broker_name!r}.",
                    )

            previous_owner = backend_owners.setdefault(
                id(broker.result_backend),
                broker.broker_name,
            )
            if previous_owner != broker.broker_name:
                raise WorkerRuntimeConfigurationError(
                    "One result backend instance cannot be lifecycle-owned by "
                    f"brokers {previous_owner!r} and {broker.broker_name!r}.",
                )

    def _validate_task_lookup(self) -> None:
        task_owners: dict[str, tuple[object, str]] = {}
        for broker in self.selection.listeners:
            for task_name, task in broker.get_all_tasks().items():
                existing = task_owners.setdefault(
                    task_name,
                    (task, broker.broker_name),
                )
                if existing[0] is not task:
                    raise WorkerRuntimeConfigurationError(
                        f"Task {task_name!r} resolves differently for brokers "
                        f"{existing[1]!r} and {broker.broker_name!r}.",
                    )
