import asyncio
from logging import getLogger

from taskiq.abc.broker import AsyncBroker
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.receiver import Receiver

logger = getLogger("taskiq.worker")


async def async_listen_messages(
    broker: AsyncBroker,
    cli_args: WorkerArgs,
) -> None:  # pragma: no cover
    """
    This function iterates over tasks asynchronously.

    It uses listen() method of an AsyncBroker
    to get new messages from queues.

    :param broker: broker to listen to.
    :param cli_args: CLI arguments for worker.
    """
    logger.debug("Runing startup event.")
    await broker.startup()
    logger.debug("Initialized receiver.")
    receiver = Receiver(broker, cli_args)
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
        logger.debug("Received {0} tasks".format(len(tasks)))
