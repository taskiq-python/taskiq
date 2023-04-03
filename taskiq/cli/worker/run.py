import asyncio
import logging
import signal
from typing import Any

from watchdog.observers import Observer

from taskiq.abc.broker import AsyncBroker
from taskiq.cli.utils import import_object, import_tasks
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.process_manager import ProcessManager
from taskiq.receiver import async_listen_messages

try:
    import uvloop  # noqa: WPS433
except ImportError:
    uvloop = None  # type: ignore


logger = logging.getLogger("taskiq.worker")


async def shutdown_broker(broker: AsyncBroker, timeout: float) -> None:
    """
    This function used to shutdown broker.

    Broker can throw erorrs during shutdown,
    or it may return some value.

    We need to handle such situations.

    :param broker: current broker.
    :param timeout: maximum amout of time to shutdown the broker.
    """
    logger.warning("Shutting down the broker.")
    try:
        ret_val = await asyncio.wait_for(broker.shutdown(), timeout)  # type: ignore
        if ret_val is not None:
            logger.info("Broker returned value on shutdown: '%s'", str(ret_val))
    except asyncio.TimeoutError:
        logger.warning("Cannot shutdown broker gracefully. Timed out.")
    except Exception as exc:
        logger.warning(
            "Exception found while terminating: %s",
            exc,
            exc_info=True,
        )


def start_listen(args: WorkerArgs) -> None:  # noqa: WPS213
    """
    This function starts actual listening process.

    It imports broker and all tasks.
    Since tasks registers themselfs in a global set,
    it's easy to just import module where you have decorated
    function and they will be available in broker's `available_tasks`
    field.

    :param args: CLI arguments.
    :raises ValueError: if broker is not an AsyncBroker instance.
    """
    if uvloop is not None:
        logger.debug("UVLOOP found. Installing policy.")
        uvloop.install()
    # This option signals that current
    # broker is running as a worker.
    # We must set this field before importing tasks,
    # so broker will remember all tasks it's related to.
    AsyncBroker.is_worker_process = True
    broker = import_object(args.broker)
    import_tasks(args.modules, args.tasks_pattern, args.fs_discover)
    if not isinstance(broker, AsyncBroker):
        raise ValueError("Unknown broker type. Please use AsyncBroker instance.")

    # Here how we manage interruptions.
    # We have to remember shutting_down state,
    # because KeyboardInterrupt can be send multiple
    # times. And it may interrupt the broker's shutdown process.
    shutting_down = False

    def interrupt_handler(signum: int, _frame: Any) -> None:
        """
        Signal handler.

        This handler checks if process is already
        terminating and if it's true, it does nothing.

        :param signum: received signal number.
        :param _frame: current execution frame.
        :raises KeyboardInterrupt: if termiation hasn't begun.
        """
        logger.debug(f"Got signal {signum}.")
        nonlocal shutting_down  # noqa: WPS420
        if shutting_down:
            return
        shutting_down = True  # noqa: WPS442
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, interrupt_handler)
    signal.signal(signal.SIGTERM, interrupt_handler)

    loop = asyncio.get_event_loop()
    try:
        coro = async_listen_messages(
            broker=broker,
            max_threadpool_threads=args.max_threadpool_threads,
            validate_params=not args.no_parse,
            max_async_tasks=args.max_async_tasks,
        )
        loop.run_until_complete(coro)
    except KeyboardInterrupt:
        logger.warning("Worker process interrupted.")
        loop.run_until_complete(shutdown_broker(broker, args.shutdown_timeout))


def run_worker(args: WorkerArgs) -> None:  # noqa: WPS213
    """
    This function starts worker processes.

    It just creates multiple child processes
    and joins them all.

    :param args: CLI arguments.
    """
    logging.basicConfig(
        level=logging.getLevelName(args.log_level),
        format="[%(asctime)s][%(name)s][%(levelname)-7s][%(processName)s] %(message)s",
    )
    logging.getLogger("watchdog.observers.inotify_buffer").setLevel(level=logging.INFO)
    logger.info("Starting %s worker processes.", args.workers)

    observer = Observer()

    if args.reload:
        observer.start()
        args.workers = 1
        logging.warning(
            "Reload on chage enabled. Number of worker processes set to 1.",
        )

    manager = ProcessManager(args=args, observer=observer, worker_function=start_listen)

    manager.start()

    if observer.is_alive():
        if args.reload:
            logger.info("Stopping watching files.")
        observer.stop()
    logger.info("Stopping logging thread.")
