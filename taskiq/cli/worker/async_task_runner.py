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
    logger.info("Runing startup event.")
    await broker.startup()
    logger.info("Inicialized receiver.")
    receiver = Receiver(broker, cli_args)
    logger.info("Listening started.")
    tasks = set()
    async for message in broker.listen():
        task = asyncio.create_task(
            receiver.callback(message=message, raise_err=False),
        )

        if cli_args.max_async_tasks > 1:
            tasks.add(task)

        if tasks and len(tasks) >= cli_args.max_async_tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            tasks.clear()
