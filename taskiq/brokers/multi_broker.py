import contextlib
from collections.abc import AsyncGenerator
from logging import getLogger
from typing import Any, NoReturn

from taskiq.abc.broker import AckableMessage, AsyncBroker
from taskiq.message import BrokerMessage
from taskiq.utils import merge_async_iterables

logger = getLogger(__name__)


def _task_registration_disabled(*args: Any, **kwargs: Any) -> NoReturn:
    """Replace the `task` method of brokers managed by a MultiBroker."""
    raise ValueError(
        "Cannot register tasks on brokers managed by a MultiBroker. "
        "Register tasks on the MultiBroker itself.",
    )


class MultiBroker(AsyncBroker):
    """
    Broker that combines multiple brokers into one.

    It wraps several real brokers and allows a single worker
    to consume messages from all of them at the same time.
    Every inner broker keeps its own connection and queue,
    this broker only merges the listening and routes kicks.

    If an inner broker fails, it's excluded from listening
    and the worker keeps consuming the remaining brokers.
    The exclusion lasts until the worker process restarts.
    When all inner brokers are down, the worker stops
    and is restarted by the supervisor.

    Tasks are registered on the MultiBroker itself and routed
    to inner brokers with the routing label:

    >>> fast_broker = AioPikaBroker(url, queue_name="fast")
    >>> slow_broker = AioPikaBroker(url, queue_name="slow")
    >>> broker = MultiBroker(
    ...     routing={"fast": fast_broker, "slow": slow_broker},
    ...     default_routing_label="fast",
    ... )

    >>> @broker.task(labels={"queue_name": "slow"})
    ... async def my_task() -> None: ...

    A single `taskiq worker` command consumes messages
    from all inner brokers this way.

    :param routing: mapping of routing label values to brokers,
        at least one entry is required. Brokers are deduplicated,
        every unique broker is listened to once.
    :param default_routing_label: label used to kick messages
        without the routing label, must be a key of `routing`.
        If it's not set, unlabeled messages are rejected.
    :param routing_label: message label used for routing,
        defaults to `queue_name`.
    :raises ValueError: if `routing` is empty or has values
        of a wrong type, or if `default_routing_label`
        has no mapped broker.
    """

    def __init__(
        self,
        routing: "dict[str, AsyncBroker] | None" = None,
        default_routing_label: str | None = None,
        routing_label: str = "queue_name",
    ) -> None:
        if not routing:
            raise ValueError(
                "MultiBroker requires a non-empty routing mapping "
                "of queue names to brokers.",
            )
        for queue_name, inner_broker in routing.items():
            if not isinstance(inner_broker, AsyncBroker):
                raise ValueError(
                    "All routed brokers must be AsyncBroker instances, "
                    f"got {type(inner_broker).__name__} for queue '{queue_name}'.",
                )
        if default_routing_label is not None and default_routing_label not in routing:
            raise ValueError(
                "There is no broker mapped for the default queue "
                f"'{default_routing_label}'. Known queues: {sorted(routing)}.",
            )
        # Inner brokers must be saved before calling super().__init__(),
        # since it sets the is_worker_process flag,
        # which propagates to all inner brokers.
        # Several labels can point to the same broker,
        # so brokers are deduplicated to listen to each of them once.
        self.brokers: list[AsyncBroker] = list(dict.fromkeys(routing.values()))
        self.routing: dict[str, AsyncBroker] = dict(routing)
        self.default_routing_label = default_routing_label
        self.routing_label = routing_label
        for inner_broker in self.brokers:
            # Tasks must live in the registry of this broker,
            # so accidental registration on inner brokers is loud.
            inner_broker.task = _task_registration_disabled  # type: ignore[method-assign]
        super().__init__()

    @property
    def is_worker_process(self) -> bool:
        """True only if broker runs in worker process."""
        return self._is_worker_process

    @is_worker_process.setter
    def is_worker_process(self, value: bool) -> None:
        self._is_worker_process = value
        for inner_broker in self.brokers:
            inner_broker.is_worker_process = value

    @property
    def is_scheduler_process(self) -> bool:
        """True only if broker runs in scheduler process."""
        return self._is_scheduler_process

    @is_scheduler_process.setter
    def is_scheduler_process(self, value: bool) -> None:
        self._is_scheduler_process = value
        for inner_broker in self.brokers:
            inner_broker.is_scheduler_process = value

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kick a message using one of the inner brokers.

        The broker is selected by the routing label of the message.
        Messages without the label go to the broker mapped
        to the default routing label.

        :param message: message to send.
        :raises ValueError: if the message has no routing label
            and no default is configured, or the label value
            has no mapped broker.
        """
        label_value = message.labels.get(self.routing_label)
        if label_value is None:
            if self.default_routing_label is None:
                raise ValueError(
                    f"Message has no '{self.routing_label}' label and the "
                    "MultiBroker has no default_routing_label, cannot route it.",
                )
            label_value = self.default_routing_label
        target_broker = self.routing.get(label_value)
        if target_broker is None:
            raise ValueError(
                f"There is no broker mapped for queue '{label_value}'. "
                f"Known queues: {sorted(self.routing)}.",
            )
        await target_broker.kick(message)

    async def listen(self) -> AsyncGenerator["bytes | AckableMessage", None]:
        """
        Listen to all inner brokers simultaneously.

        This method merges listen generators of all inner brokers
        into a single generator, so the worker receives messages
        from every broker at the same time.

        If one of the inner brokers fails, it's excluded from
        listening, but all remaining brokers keep working. Only when
        every inner broker is down, this generator ends and the worker
        restarts.

        :yield: incoming messages from all inner brokers.
        :return: nothing.
        """
        merged = merge_async_iterables(
            *(self._listen_one(inner_broker) for inner_broker in self.brokers),
        )
        async for message in merged:
            yield message

    async def _listen_one(
        self,
        inner_broker: AsyncBroker,
    ) -> AsyncGenerator["bytes | AckableMessage", None]:
        """
        Listen to a single inner broker, degrading on its failure.

        If the inner broker's listen fails, the error is logged
        and this generator ends, so the failed broker is excluded
        from the merged listening, but doesn't stop the whole worker.

        :param inner_broker: broker to listen to.
        :yield: incoming messages from the inner broker.
        :return: nothing.
        """
        iterator = inner_broker.listen()
        try:
            async for message in iterator:
                yield message
        except Exception as exc:
            logger.exception(
                "Inner broker %s failed: %r. "
                "It's excluded from listening, "
                "continuing with remaining brokers.",
                inner_broker,
                exc,
            )
        finally:
            # Catches both the failure case and the outer shutdown:
            # a suspended generator must run its cleanup logic.
            with contextlib.suppress(RuntimeError):
                await iterator.aclose()

    async def startup(self) -> None:
        """
        Startup the composite broker and all inner brokers.

        Event handlers and middlewares of this broker are called
        first, then every inner broker is started.
        """
        await super().startup()
        for inner_broker in self.brokers:
            await inner_broker.startup()

    async def shutdown(self) -> None:
        """
        Shut down all inner brokers and the composite broker.

        Exceptions raised by inner brokers are logged
        and don't prevent other brokers from shutting down.
        """
        for inner_broker in self.brokers:
            try:
                await inner_broker.shutdown()
            except Exception as exc:
                logger.warning(
                    "Cannot shutdown broker %s: %s",
                    inner_broker,
                    exc,
                    exc_info=True,
                )
        await super().shutdown()
