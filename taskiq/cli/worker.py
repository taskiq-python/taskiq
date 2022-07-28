import asyncio
from importlib import import_module
from logging import basicConfig, getLevelName, getLogger
from multiprocessing import Process
from pathlib import Path
from typing import Any

from taskiq.abc.broker import AsyncBroker
from taskiq.cli.args import TaskiqArgs
from taskiq.cli.task_runner import async_listen_messages

logger = getLogger("taskiq.worker")


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


async def run_worker(args: TaskiqArgs) -> None:
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
    worker_processes = []
    for worker_num in range(args.workers):
        work_proc = Process(
            target=start_listen,
            kwargs={"args": args},
            name=f"worker-{worker_num}",
        )
        work_proc.start()
        logger.debug(
            "Started process worker-%d with pid %s "
            % (
                worker_num,
                work_proc.pid,
            ),
        )
        worker_processes.append(work_proc)

    for wp in worker_processes:
        wp.join()
