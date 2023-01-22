import asyncio
import os
import signal
import sys
from logging import StreamHandler, basicConfig, getLevelName, getLogger
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Process, Queue
from time import sleep
from typing import Any, List

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


logger = getLogger("taskiq.worker")


restart_workers = True
worker_processes: List[Process] = []
observer = Observer()
reload_queue: "Queue[bool]" = Queue(-1)


def signal_handler(_signal: int, _frame: Any) -> None:
    """
    This handler is used only by main process.

    If the OS sent you SIGINT or SIGTERM,
    we should kill all spawned processes.

    :param _signal: incoming signal.
    :param _frame: current execution frame.
    """
    global restart_workers  # noqa: WPS420
    global worker_processes  # noqa: WPS420

    restart_workers = False  # noqa: WPS442
    for process in worker_processes:
        # This is how we kill children,
        # by sending SIGINT to child processes.
        if process.pid is None:
            continue
        try:
            os.kill(process.pid, signal.SIGINT)
        except ProcessLookupError:
            continue
        process.join()
    if observer.is_alive():
        observer.stop()
        observer.join()


def schedule_workers_reload() -> None:
    """
    Function to schedule workers to restart.

    This function adds worker ids to the queue.

    This queue is later read in watcher loop.
    """
    global worker_processes  # noqa: WPS420
    global reload_queue  # noqa: WPS420

    reload_queue.put(True)
    logger.info("Scheduled workers reload.")
    reload_queue.join()


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


def start_listen(args: WorkerArgs) -> None:  # noqa: C901
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

    def interrupt_handler(_signum: int, _frame: Any) -> None:
        """
        Signal handler.

        This handler checks if process is already
        terminating and if it's true, it does nothing.

        :param _signum: received signal number.
        :param _frame: current execution frame.
        :raises KeyboardInterrupt: if termiation hasn't begun.
        """
        nonlocal shutting_down  # noqa: WPS420
        if shutting_down:
            return
        shutting_down = True  # noqa: WPS442
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, interrupt_handler)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(async_listen_messages(broker, args))
    except KeyboardInterrupt:
        logger.warning("Worker process interrupted.")
        loop.run_until_complete(shutdown_broker(broker, args.shutdown_timeout))


def watcher_loop(args: WorkerArgs) -> None:  # noqa: C901, WPS213
    """
    Infinate loop for main process.

    This loop restarts worker processes
    if they exit with error returncodes.

    Also it reads process ids from reload_queue
    and reloads workers if they were scheduled to reload.

    :param args: cli arguements.
    """
    global worker_processes  # noqa: WPS420
    global restart_workers  # noqa: WPS420

    while worker_processes and restart_workers:
        # List of processes to remove.
        sleep(1)
        process_to_remove = []
        if not reload_queue.empty():
            while not reload_queue.empty():
                reload_queue.get()
                reload_queue.task_done()

            for worker_id, worker in enumerate(worker_processes):
                worker.terminate()
                worker.join()
                worker_processes[worker_id] = Process(
                    target=start_listen,
                    kwargs={"args": args},
                    name=f"worker-{worker_id}",
                )
                worker_processes[worker_id].start()

        for worker_id, worker in enumerate(worker_processes):
            if worker.is_alive():
                continue
            if worker.exitcode is not None and worker.exitcode > 0 and restart_workers:
                logger.info("Trying to restart the worker-%s", worker_id)
                worker_processes[worker_id] = Process(
                    target=start_listen,
                    kwargs={"args": args},
                    name=f"worker-{worker_id}",
                )
                worker_processes[worker_id].start()
            else:
                logger.info("Worker-%s terminated.", worker_id)
                process_to_remove.append(worker)

        for dead_process in process_to_remove:
            worker_processes.remove(dead_process)


def run_worker(args: WorkerArgs) -> None:  # noqa: WPS213
    """
    This function starts worker processes.

    It just creates multiple child processes
    and joins them all.

    :param args: CLI arguments.
    """
    logging_queue = Queue(-1)  # type: ignore
    listener = QueueListener(logging_queue, StreamHandler(sys.stdout))
    basicConfig(
        level=getLevelName(args.log_level),
        format="[%(asctime)s][%(levelname)-7s][%(processName)s] %(message)s",
        handlers=[QueueHandler(logging_queue)],
    )
    listener.start()
    logger.info("Starting %s worker processes.", args.workers)

    global worker_processes  # noqa: WPS420

    for process in range(args.workers):
        work_proc = Process(
            target=start_listen,
            kwargs={"args": args},
            name=f"worker-{process}",
        )
        work_proc.start()
        logger.debug(
            "Started process worker-%d with pid %s ",
            process,
            work_proc.pid,
        )
        worker_processes.append(work_proc)

    if args.reload:
        observer.schedule(
            FileWatcher(
                callback=schedule_workers_reload,
                use_gitignore=not args.no_gitignore,
            ),
            path=".",
            recursive=True,
        )
        observer.start()
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    watcher_loop(args=args)
    listener.stop()
