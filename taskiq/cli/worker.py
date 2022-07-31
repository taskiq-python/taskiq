import asyncio
import signal
from importlib import import_module
from logging import basicConfig, getLevelName, getLogger
from multiprocessing import Process
from pathlib import Path
from time import sleep
from typing import Any, List

from taskiq.abc.broker import AsyncBroker
from taskiq.cli.args import TaskiqArgs
from taskiq.cli.async_task_runner import async_listen_messages

logger = getLogger("taskiq.worker")


restart_workers = True
worker_processes: List[Process] = []


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
        # This is how we send SIGTERM to child
        # processes.
        process.terminate()
        process.join()


def import_broker(broker_spec: str) -> Any:
    """
    It parses broker spec and imports it.

    :param broker_spec: string in format like `package.module:variable`
    :raises ValueError: if spec has unknown format.
    :returns: imported broker.
    """
    import_spec = broker_spec.split(":")
    if len(import_spec) != 2:
        raise ValueError("You should provide broker in `module:variable` format.")
    module = import_module(import_spec[0])
    return getattr(module, import_spec[1])


def import_from_modules(modules: list[str]) -> None:
    """
    Import all modules from modules variable.

    :param modules: list of modules.
    """
    for module in modules:
        try:
            logger.info(f"Importing tasks from module {module}")
            import_module(module)
        except ImportError:
            logger.warning(f"Cannot import {module}")


def import_tasks(modules: list[str], pattern: str, fs_discover: bool) -> None:
    """
    Import tasks modules.

    This function is used to
    import all tasks from modules.

    :param modules: list of modules to import.
    :param pattern: pattern of a file if fs_discover is True.
    :param fs_discover: If true it will try to import modules
        from filesystem.
    """
    if fs_discover:
        for path in Path(".").rglob(pattern):
            modules.append(str(path).removesuffix(".py").replace("/", "."))

    import_from_modules(modules)


def start_listen(args: TaskiqArgs) -> None:
    """
    This function starts actual listening process.

    It imports broker and all tasks.
    Since tasks registers themselfs in broker
    it's easy to just import module where you have decorated
    function and they will be available in broker's `_related_tasks`
    field.

    :param args: CLI arguments.
    :raises ValueError: if broker is not an AsyncBroker instance.
    """
    broker = import_broker(args.broker)
    # This option signals that current
    # broker is running as a worker.
    # We must set this field before importing tasks,
    # so broker will remember all tasks it's related to.
    broker.is_worker_process = True
    import_tasks(args.modules, args.tasks_pattern, args.fs_discover)
    if isinstance(broker, AsyncBroker):
        asyncio.run(async_listen_messages(broker, args))
    else:
        raise ValueError("Unknown broker type. Please use AsyncBroker instance.")


def watch_workers_restarts(args: TaskiqArgs) -> None:
    """
    Infinate loop for main process.

    This loop restarts worker processes
    if they exit with error returncodes.

    :param args: cli arguements.
    """
    global worker_processes  # noqa: WPS420
    global restart_workers  # noqa: WPS420

    while worker_processes and restart_workers:
        # List of processes to remove.
        sleep(1)
        process_to_remove = []
        for worker_id, worker in enumerate(worker_processes):
            if worker.is_alive():
                continue
            if worker.exitcode is not None and worker.exitcode > 0 and restart_workers:
                logger.info("Trying to restart the worker-%s" % worker_id)
                worker_processes[worker_id] = Process(
                    target=start_listen,
                    kwargs={"args": args},
                    name=f"worker-{worker_id}",
                )
                worker_processes[worker_id].start()
            else:
                logger.info("Worker-%s has finished." % worker_id)
                worker.join()
                process_to_remove.append(worker)

        for dead_process in process_to_remove:
            worker_processes.remove(dead_process)


def run_worker(args: TaskiqArgs) -> None:
    """
    This function starts worker processes.

    It just creates multiple child processes
    and joins them all.

    :param args: CLI arguments.
    """
    basicConfig(
        level=getLevelName(args.log_level),
        format=("[%(asctime)s][%(levelname)-7s][%(processName)s] %(message)s"),
    )
    logger.info("Starting %s worker processes." % args.workers)

    global worker_processes  # noqa: WPS420

    for process in range(args.workers):
        work_proc = Process(
            target=start_listen,
            kwargs={"args": args},
            name=f"worker-{process}",
        )
        work_proc.start()
        logger.debug(
            "Started process worker-%d with pid %s "
            % (
                process,
                work_proc.pid,
            ),
        )
        worker_processes.append(work_proc)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    watch_workers_restarts(args=args)
