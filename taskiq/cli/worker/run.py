import asyncio
import enum
import logging
import signal
import sys
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Process, Queue
from time import sleep
from typing import Any, Callable, List, Optional, Tuple

from watchdog.observers import Observer

from taskiq.abc.broker import AsyncBroker
from taskiq.cli.utils import import_object, import_tasks
from taskiq.cli.watcher import FileWatcher
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.async_task_runner import async_listen_messages

try:
    import uvloop  # noqa: WPS433
except ImportError:
    uvloop = None  # type: ignore


class ProcessAction(enum.Enum):
    """Enumberate of possible events."""

    FULL_RELOAD = enum.auto()
    SHUTDOWN = enum.auto()
    RELOAD_ONE = enum.auto()


logger = logging.getLogger("taskiq.worker")


def get_signal_handler(
    action_queue: "Queue[Tuple[ProcessAction, Optional[int]]]",
) -> Callable[[int, Any], None]:
    """
    Generate singnal handler for main process.

    The signal handler will just put the SHUTDOWN event in
    the action queue.

    :param action_queue: event queue.
    :returns: actual signal handler.
    """

    def _signal_handler(signal_num: int, _frame: Any) -> None:
        logger.debug(f"Got signal {signal_num}.")
        action_queue.put((ProcessAction.SHUTDOWN, None))
        logger.warn("Workers are scheduled for shutdown.")

    return _signal_handler


def schedule_workers_reload(
    action_queue: "Queue[Tuple[ProcessAction, Optional[int]]]",
) -> None:
    """
    Function to schedule workers to restart.

    It simply send FULL_RELOAD event, which is handled
    in the mainloop.

    :param action_queue: queue to send events to.
    """
    action_queue.put((ProcessAction.FULL_RELOAD, None))
    logger.info("Scheduled workers reload.")


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


def start_listen(args: WorkerArgs) -> None:  # noqa: C901, WPS213
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
        loop.run_until_complete(async_listen_messages(broker, args))
    except KeyboardInterrupt:
        logger.warning("Worker process interrupted.")
        loop.run_until_complete(shutdown_broker(broker, args.shutdown_timeout))


def start_process_watcher(  # noqa: C901, WPS210, WPS213
    args: WorkerArgs,
    action_queue: "Queue[Tuple[ProcessAction, Optional[int]]]",
) -> None:
    """
    Main loop.

    This function is an endless loop,
    which listens to new events from different sources.

    Every second it checks for new events and
    current states of child processes.

    If there are new events it handles them.
    Taskiq has 3 types of events:

    1. FULL_RELOAD - when we want to restart all child processes.
        It checks for running processes and generates RELOAD_ONE event for
        any process.

    2. RELOAD_ONE - this event restarts one single child process.

    3. SHUTDOWN - exits the loop. Since all child processes are
        daemons, they will be automatically terminated using signals.

    If some processes are not up, it schedules a restart
    for each unhealthy process.

    :param args: cli args for worker.
    :param action_queue: queue for events.
    """
    workers: List[Process] = []
    for process in range(args.workers):
        work_proc = Process(
            target=start_listen,
            kwargs={"args": args},
            name=f"worker-{process}",
            daemon=True,
        )
        logger.debug(
            "Started process worker-%d with pid %s ",
            process,
            work_proc.pid,
        )
        work_proc.start()
        workers.append(work_proc)

    while True:
        sleep(1)
        reloaded_workers = set()
        # We bulk_process all pending events.
        while not action_queue.empty():
            action, action_arg = action_queue.get()
            logging.debug(f"GOT event: {action} with args: {action_arg}")
            if action == ProcessAction.FULL_RELOAD:
                # Generate RELOAD_ONE for all current children.
                for worker_num, _ in enumerate(workers):
                    action_queue.put((ProcessAction.RELOAD_ONE, worker_num))
            elif action == ProcessAction.RELOAD_ONE:
                if action_arg is None:
                    continue
                # If we just reloaded this worker, skip handling.
                if action_arg in reloaded_workers:
                    continue
                # Check that it's a valid worker id.
                if action_arg < 0 or action_arg >= len(workers):
                    logger.warn(f"Unknown worker number: {action_arg}.")
                    continue
                worker = workers[action_arg]
                try:
                    worker.terminate()
                except ValueError:
                    logger.debug(f"Process {worker.name} is already terminated.")
                # Waiting worker shutdown.
                worker.join()
                new_process = Process(
                    target=start_listen,
                    kwargs={"args": args},
                    name=f"worker-{action_arg}",
                    daemon=True,
                )
                new_process.start()
                workers[action_arg] = new_process
                reloaded_workers.add(action_arg)
            elif action == ProcessAction.SHUTDOWN:
                logger.debug("Event processed.")
                return

        for worker_num, worker in enumerate(workers):
            if not worker.is_alive():
                logger.info(f"{worker.name} is dead. Scheduling reload.")
                action_queue.put((ProcessAction.RELOAD_ONE, worker_num))


def run_worker(args: WorkerArgs) -> None:  # noqa: WPS213
    """
    This function starts worker processes.

    It just creates multiple child processes
    and joins them all.

    :param args: CLI arguments.
    """
    logging_queue = Queue(-1)  # type: ignore
    listener = QueueListener(logging_queue, logging.StreamHandler(sys.stdout))
    logging.basicConfig(
        level=logging.getLevelName(args.log_level),
        format="[%(asctime)s][%(name)s][%(levelname)-7s][%(processName)s] %(message)s",
        handlers=[QueueHandler(logging_queue)],
    )
    logging.getLogger("watchdog.observers.inotify_buffer").setLevel(level=logging.INFO)
    listener.start()
    logger.info("Starting %s worker processes.", args.workers)

    action_queue: "Queue[Tuple[ProcessAction, Optional[int]]]" = Queue(-1)
    observer = Observer()

    if args.reload:
        observer.schedule(
            FileWatcher(
                callback=schedule_workers_reload,
                use_gitignore=not args.no_gitignore,
                action_queue=action_queue,
            ),
            path=".",
            recursive=True,
        )
        observer.start()
        logging.warning("Reload on chage enabled. Number of worker processes set to 1.")
        args.workers = 1

    signal_handler = get_signal_handler(action_queue)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    start_process_watcher(args, action_queue)

    if observer.is_alive():
        if args.reload:
            logger.info("Stopping watching files.")
        observer.stop()
    logger.info("Stopping logging thread.")
    listener.stop()
