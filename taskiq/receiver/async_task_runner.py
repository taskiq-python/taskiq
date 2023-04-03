import asyncio
from concurrent.futures.thread import ThreadPoolExecutor
from logging import getLogger

from taskiq.abc.broker import AsyncBroker
from taskiq.receiver.receiver import Receiver

logger = getLogger("taskiq.worker")


async def async_listen_messages(
    broker: AsyncBroker,
    max_threadpool_threads: int = 10,
    validate_params: bool = True,
    max_async_tasks: int = 20,
) -> None:  # pragma: no cover
    """
    This function iterates over tasks asynchronously.

    It uses listen() method of an AsyncBroker
    to get new messages from queues.

    :param broker: broker to listen to.
    :param max_threadpool_threads: maximum threads for executing sync functions.
    :param validate_params: whether it is to validate the types of incoming params.
    :param max_async_tasks: maximum async tasks per worker.
    """
    logger.debug("Runing startup event.")
    await broker.startup()
    logger.debug("Initialized receiver.")
    with ThreadPoolExecutor(max_threadpool_threads) as pool:
        receiver = Receiver(broker, pool, validate_params, max_async_tasks)
        logger.info("Listening started.")
        tasks = set()
        async for message in broker.listen():
            task = asyncio.create_task(
                receiver.callback(message=message, raise_err=False),
            )
            tasks.add(task)

            # We want the task to remove itself from the set when it's done.
            #
            # Because python's GC can silently cancel task
            # and it considered to be Hisenbug.
            # https://textual.textualize.io/blog/2023/02/11/the-heisenbug-lurking-in-your-async-code/
            task.add_done_callback(tasks.discard)
