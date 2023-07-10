import asyncio
import logging
import signal
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.synchronize import Event
from typing import Any, Type

from taskiq.abc.broker import AsyncBroker
from taskiq.cli.utils import import_object, import_tasks
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.process_manager import ProcessManager
from taskiq.receiver import Receiver

try:
    import uvloop  # noqa: WPS433
except ImportError:
    uvloop = None  # type: ignore


try:
    from watchdog.observers import Observer  # noqa: WPS433
except ImportError:
    Observer = None  # type: ignore

logger = logging.getLogger("taskiq.worker")


async def shutdown_broker(broker: AsyncBroker, timeout: float) -> None:
    """
    This function used to shutdown broker.

    Broker can throw errors during shutdown,
    or it may return some value.

    We need to handle such situations.

    :param broker: current broker.
    :param timeout: maximum amount of time to shutdown the broker.
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


def get_receiver_type(args: WorkerArgs) -> Type[Receiver]:
    """
    Import Receiver from args.

    :param args: CLI arguments.
    :raises ValueError: if receiver is not a Receiver type.
    :return: Receiver type.
    """
    receiver_type = import_object(args.receiver)
    if not (isinstance(receiver_type, type) and issubclass(receiver_type, Receiver)):
        raise ValueError("Unknown receiver type. Please use Receiver class.")
    return receiver_type


def start_listen(args: WorkerArgs, event: Event) -> None:  # noqa: WPS210, WPS213
    """
    This function starts actual listening process.

    It imports broker and all tasks.
    Since tasks registers themselves in a global set,
    it's easy to just import module where you have decorated
    function and they will be available in broker's `available_tasks`
    field.

    :param args: CLI arguments.
    :param event: Event for notification.
    :raises ValueError: if broker is not an AsyncBroker instance.
    :raises ValueError: if receiver is not a Receiver type.
    """
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
        :raises KeyboardInterrupt: if termination hasn't begun.
        """
        logger.debug(f"Got signal {signum}.")
        nonlocal shutting_down  # noqa: WPS420
        if shutting_down:
            return
        shutting_down = True  # noqa: WPS442
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, interrupt_handler)
    signal.signal(signal.SIGTERM, interrupt_handler)

    # Notify parent process, worker is ready
    event.set()

    if uvloop is not None:
        logger.debug("UVLOOP found. Installing policy.")
        uvloop.install()
    # This option signals that current
    # broker is running as a worker.
    # We must set this field before importing tasks,
    # so broker will remember all tasks it's related to.
    broker = import_object(args.broker)
    if not isinstance(broker, AsyncBroker):
        raise ValueError("Unknown broker type. Please use AsyncBroker instance.")
    broker.is_worker_process = True
    import_tasks(args.modules, args.tasks_pattern, args.fs_discover)

    receiver_type = get_receiver_type(args)
    receiver_args = dict(args.receiver_arg)

    loop = asyncio.get_event_loop()

    try:
        logger.debug("Initialize receiver.")
        with ThreadPoolExecutor(args.max_threadpool_threads) as pool:
            receiver = receiver_type(
                broker=broker,
                executor=pool,
                validate_params=not args.no_parse,
                max_async_tasks=args.max_async_tasks,
                max_prefetch=args.max_prefetch,
                propagate_exceptions=not args.no_propagate_errors,
                **receiver_args,
            )
            loop.run_until_complete(receiver.listen())
    except KeyboardInterrupt:
        logger.warning("Worker process interrupted.")
        loop.run_until_complete(shutdown_broker(broker, args.shutdown_timeout))


def run_worker(args: WorkerArgs) -> None:  # noqa: WPS213
    """
    This function starts worker processes.

    It just creates multiple child processes
    and joins them all.

    :param args: CLI arguments.

    :raises ValueError: if reload flag is used, but dependencies are not installed.
    """
    if args.configure_logging:
        logging.basicConfig(
            level=logging.getLevelName(args.log_level),
            format="[%(asctime)s][%(name)s][%(levelname)-7s]"
            + "[%(processName)s] %(message)s",
        )
    logging.getLogger("taskiq").setLevel(level=logging.getLevelName(args.log_level))
    logging.getLogger("watchdog.observers.inotify_buffer").setLevel(level=logging.INFO)
    logger.info("Starting %s worker processes.", args.workers)

    observer = None

    if args.reload and Observer is None:
        raise ValueError("To use '--reload' flag, please install 'taskiq[reload]'.")

    if Observer is not None and args.reload:
        observer = Observer()
        observer.start()
        args.workers = 1
        logging.warning(
            "Reload on change enabled. Number of worker processes set to 1.",
        )

    manager = ProcessManager(args=args, observer=observer, worker_function=start_listen)

    manager.start()

    if observer is not None and observer.is_alive():
        if args.reload:
            logger.info("Stopping watching files.")
        observer.stop()
    logger.info("Stopping logging thread.")
