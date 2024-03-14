import asyncio
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from typing import Optional, Type

from taskiq.abc.broker import AsyncBroker
from taskiq.acks import AcknowledgeType
from taskiq.receiver.receiver import Receiver

logger = getLogger("taskiq.receiver")


async def run_receiver_task(
    broker: AsyncBroker,
    receiver_cls: Type[Receiver] = Receiver,
    sync_workers: int = 4,
    validate_params: bool = True,
    max_async_tasks: int = 100,
    max_prefetch: int = 0,
    propagate_exceptions: bool = True,
    run_startup: bool = False,
    ack_time: Optional[AcknowledgeType] = None,
) -> None:
    """
    Function to run receiver programmatically.

    This function helps people to dynamically define brokers
    and start listening to them within their own code,
    without using taskiq worker command.

    This command is less robust and it doesn't have
    multiprocessing support. Because it's not always
    possible to use daemon processes in some environments.
    Because daemon processes can't spawn their own children.

    To use it, you can create an asyncio task and run it
    in background.


    :param broker: current broker instance.
    :param receiver_cls: receiver class to use.
    :param sync_workers: number of threads of a threadpool that runs sync tasks.
    :param validate_params: whether to validate params or not.
    :param max_async_tasks: maximum number of simultaneous async tasks.
    :param max_prefetch: maximum number of tasks to prefetch.
    :param propagate_exceptions: whether to propagate exceptions in generators or not.
    :param run_startup: whether to run startup function or not.
    :param ack_time: acknowledge type to use.
    :raises asyncio.CancelledError: if the task was cancelled.
    """

    def on_exit(_: Receiver) -> None:
        """
        Function for compatibility with older versions of anyio.

        On older versions taskgroup doesn't throw CancelledError
        when any task within the group was cancelled.

        :raises CancelledError: always.
        """
        raise asyncio.CancelledError

    with ThreadPoolExecutor(max_workers=sync_workers) as executor:
        broker.is_worker_process = True
        while True:
            try:
                receiver = receiver_cls(
                    broker=broker,
                    executor=executor,
                    run_starup=run_startup,
                    validate_params=validate_params,
                    max_async_tasks=max_async_tasks,
                    max_prefetch=max_prefetch,
                    propagate_exceptions=propagate_exceptions,
                    on_exit=on_exit,
                    ack_type=ack_time,
                )
                await receiver.listen()
            except asyncio.CancelledError:
                logger.warning("The listenig task was cancelled.")
                raise
            except Exception as exc:
                logger.warning(
                    "Exception found while listening to the broker. %s",
                    exc,
                    exc_info=True,
                )
