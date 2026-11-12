import contextlib
import sys
from collections.abc import AsyncGenerator
from logging import getLogger
from typing import TYPE_CHECKING, Any

from taskiq.abc.broker import AckableMessage, AsyncBroker
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.message import BrokerMessage
from taskiq.utils import merge_async_iterables

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.result_backend import AsyncResultBackend

logger = getLogger(__name__)


class MultiBroker(AsyncBroker):
    """
    Broker that combines multiple brokers into one.

    It wraps several real brokers and allows a single worker
    to consume messages from all of them at the same time.
    Every inner broker keeps its own connection and queue,
    this broker only merges the listening and routes kicks.

    Example of running one worker for multiple queues:

    >>> fast_broker = AioPikaBroker(url, queue_name="fast")
    >>> slow_broker = AioPikaBroker(url, queue_name="slow")
    >>> broker = MultiBroker(fast_broker, slow_broker)

    Tasks must be registered on inner brokers in this case,
    and a single `taskiq worker` command will consume
    messages from both queues.

    It's also possible to register tasks on the MultiBroker
    itself. To route such tasks, pass a `routing` mapping and
    mark tasks with the `queue_name` label:

    >>> broker = MultiBroker(
    ...     fast_broker,
    ...     slow_broker,
    ...     routing={"fast": fast_broker, "slow": slow_broker},
    ... )

    :param brokers: brokers to combine, at least one is required.
    :param default_broker: broker used to kick messages
        without a routing label, defaults to the first broker.
    :param routing: mapping of routing label values to brokers.
    :param routing_label: message label used for routing, defaults to `queue_name`.
    :raises ValueError: if no brokers are provided,
        or any of them has a wrong type.
    """

    def __init__(
        self,
        *brokers: AsyncBroker,
        default_broker: AsyncBroker | None = None,
        routing: "dict[str, AsyncBroker] | None" = None,
        routing_label: str = "queue_name",
    ) -> None:
        if not brokers:
            raise ValueError("MultiBroker requires at least one broker.")
        for inner_broker in brokers:
            if not isinstance(inner_broker, AsyncBroker):
                raise ValueError(
                    "All brokers must be AsyncBroker instances, "
                    f"got {type(inner_broker).__name__}.",
                )
        if default_broker is not None and not isinstance(
            default_broker,
            AsyncBroker,
        ):
            raise ValueError(
                "default_broker must be an AsyncBroker instance, "
                f"got {type(default_broker).__name__}.",
            )
        # Inner brokers must be saved before calling super().__init__(),
        # since it sets the is_worker_process flag,
        # which propagates to all inner brokers.
        self.brokers: list[AsyncBroker] = list(brokers)
        self.default_broker = default_broker or self.brokers[0]
        self.routing: dict[str, AsyncBroker] = routing or {}
        self.routing_label = routing_label
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

        If the message has a routing label and a routing mapping
        is configured, then the broker mapped to the label value
        is used. In any other case the default broker is used.

        :param message: message to send.
        :raises ValueError: if the routing label value
            has no mapped broker.
        """
        label_value = message.labels.get(self.routing_label)
        if label_value is not None and self.routing:
            target_broker = self.routing.get(label_value)
            if target_broker is None:
                raise ValueError(
                    f"There is no broker mapped for queue '{label_value}'. "
                    f"Known queues: {sorted(self.routing)}.",
                )
        else:
            target_broker = self.default_broker
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

    def find_task(
        self,
        task_name: str,
    ) -> "AsyncTaskiqDecoratedTask[Any, Any] | None":
        """
        Search for a task in the composite broker and inner brokers.

        :param task_name: name of a task.
        :returns: found task or None.
        """
        task = self.local_task_registry.get(task_name)
        if task is not None:
            return task
        for inner_broker in self.brokers:
            task = inner_broker.find_task(task_name)
            if task is not None:
                return task
        return self.global_task_registry.get(task_name)

    def get_all_tasks(self) -> "dict[str, AsyncTaskiqDecoratedTask[Any, Any]]":
        """
        Fetch all tasks available in the composite and inner brokers.

        :return: dict of all tasks. Keys are task names,
            values are tasks.
        """
        all_tasks = {
            **self.global_task_registry,
            **self.local_task_registry,
        }
        for inner_broker in self.brokers:
            all_tasks.update(inner_broker.get_all_tasks())
        return all_tasks

    def with_result_backend(
        self,
        result_backend: "AsyncResultBackend[Any]",
    ) -> Self:
        """
        Set a result backend for the composite and all inner brokers.

        Inner brokers use their result backend to serve
        `wait_result` calls on the client side, while the worker
        saves results using the result backend of this broker.
        Setting the same backend everywhere keeps them in sync.

        :param result_backend: new result backend.
        :return: self
        """
        self.result_backend = result_backend
        for inner_broker in self.brokers:
            inner_broker.result_backend = result_backend
        return self
