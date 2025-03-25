import asyncio
import inspect
import logging
import os
import signal
import sys
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import set_start_method
from sys import platform
from typing import Any, Optional, Type

from taskiq.abc.broker import AsyncBroker
from taskiq.cli.utils import import_object, import_tasks
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.process_manager import ProcessManager
from taskiq.receiver import Receiver

try:
    import uvloop
except ImportError:
    uvloop = None  # type: ignore


try:
    from watchdog.observers import Observer
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
            logger.info("Broker has returned value on shutdown: '%s'", str(ret_val))
    except asyncio.TimeoutError:
        logger.warning("Broker.shutdown cannot be completed in %s seconds.", timeout)
    except Exception as exc:
        logger.warning(
            "Exception found while shutting down broker: %s",
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


def start_listen(args: WorkerArgs) -> None:
    """
    This function starts actual listening process.

    It imports broker and all tasks.
    Since tasks auto registeres themselves in a broker,
    we don't need to do anything else other than importing.


    :param args: CLI arguments.
    :param event: Event for notification.
    :raises ValueError: if broker is not an AsyncBroker instance.
    :raises ValueError: if receiver is not a Receiver type.
    """
    shutdown_event = asyncio.Event()
    hardkill_counter = 0

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
        nonlocal shutdown_event
        nonlocal hardkill_counter
        # Soft kill is a signal to start shutdown.
        shutdown_event.set()
        # Hard kill is a signal that we should stop
        # everything immediately.
        if hardkill_counter > args.hardkill_count:
            logger.warning("Hard kill. Exiting.")
            raise KeyboardInterrupt
        hardkill_counter += 1

    signal.signal(signal.SIGINT, interrupt_handler)
    signal.signal(signal.SIGTERM, interrupt_handler)
    if sys.platform != "win32":
        signal.signal(signal.SIGHUP, interrupt_handler)

    if uvloop is not None:
        logger.debug("UVLOOP found. Using it as async runner")
        loop = uvloop.new_event_loop()  # type: ignore
    else:
        loop = asyncio.new_event_loop()

    asyncio.set_event_loop(loop)

    # This option signals that current
    # broker is running as a worker.
    # We must set this field before importing tasks,
    # so broker will remember all tasks it's related to.

    broker = import_object(args.broker)
    if inspect.isfunction(broker):
        broker = broker()
    if not isinstance(broker, AsyncBroker):
        raise ValueError(
            "Unknown broker type. Please use AsyncBroker instance "
            "or pass broker factory function that returns an AsyncBroker instance.",
        )

    broker.is_worker_process = True
    import_tasks(args.modules, args.tasks_pattern, args.fs_discover)

    receiver_type = get_receiver_type(args)
    receiver_kwargs = dict(args.receiver_arg)

    executor: Executor
    if args.use_process_pool:
        executor = ProcessPoolExecutor(max_workers=args.max_process_pool_processes)
    else:
        executor = ThreadPoolExecutor(max_workers=args.max_threadpool_threads)

    try:
        logger.debug("Initialize receiver.")
        with executor as pool:
            receiver = receiver_type(
                broker=broker,
                executor=pool,
                validate_params=not args.no_parse,
                max_async_tasks=args.max_async_tasks,
                max_prefetch=args.max_prefetch,
                propagate_exceptions=not args.no_propagate_errors,
                ack_type=args.ack_type,
                max_tasks_to_execute=args.max_tasks_per_child,
                wait_tasks_timeout=args.wait_tasks_timeout,
                **receiver_kwargs,  # type: ignore
            )
            loop.run_until_complete(receiver.listen(shutdown_event))
    finally:
        loop.run_until_complete(shutdown_broker(broker, args.shutdown_timeout))


def run_worker(args: WorkerArgs) -> Optional[int]:
    """
    This function starts worker processes.

    It just creates multiple child processes
    and joins them all.

    :param args: CLI arguments.

    :raises ValueError: if reload flag is used, but dependencies are not installed.
    :returns: Optional status code.
    """
    if platform == "darwin":
        set_start_method("spawn")
    if args.configure_logging:
        logging.basicConfig(
            level=logging.getLevelName(args.log_level),
            format="[%(asctime)s][%(name)s][%(levelname)-7s]"
            "[%(processName)s] %(message)s",
        )
    logging.getLogger("taskiq").setLevel(level=logging.getLevelName(args.log_level))
    logging.getLogger("watchdog.observers.inotify_buffer").setLevel(level=logging.INFO)
    logger.info("Pid of a main process: %s", str(os.getpid()))
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

    status = manager.start()

    if observer is not None and observer.is_alive():
        if args.reload:
            logger.info("Stopping watching files.")
        observer.stop()

    return status
