import logging
import signal
from dataclasses import dataclass
from multiprocessing import Process, Queue
from time import sleep
from typing import Any, Callable, List

from watchdog.observers import Observer

from taskiq.cli.watcher import FileWatcher
from taskiq.cli.worker.args import WorkerArgs

logger = logging.getLogger("taskiq.process-manager")


class ProcessActionBase:
    """Base for all process actions. Used for types."""


@dataclass
class ReloadAllAction(ProcessActionBase):
    """This action triggers reload of all workers."""

    def handle(
        self,
        workers_num: int,
        action_queue: "Queue[ProcessActionBase]",
    ) -> None:
        """
        Handle reload all action.

        This action sends N reloadOne actions in a queue,
        where N is a number of worker processes.

        :param workers_num: number of currently active workers.
        :param action_queue: queue to send events to.
        """
        for worker_id in range(workers_num):
            action_queue.put(ReloadOneAction(worker_num=worker_id))


@dataclass
class ReloadOneAction(ProcessActionBase):
    """This action reloads single worker with particular id."""

    worker_num: int

    def handle(
        self,
        workers: List[Process],
        args: WorkerArgs,
        worker_func: Callable[[WorkerArgs], None],
    ) -> None:
        """
        This action reloads a single process.

        :param workers: known children processes.
        :param args: args for new process.
        :param worker_func: function that is used to start worker processes.
        """
        if self.worker_num < 0 or self.worker_num >= len(workers):
            logger.warning("Unknown worker id.")
            return
        worker = workers[self.worker_num]
        try:
            worker.terminate()
        except ValueError:
            logger.debug(f"Process {worker.name} is already terminated.")
        # Waiting worker shutdown.
        worker.join()
        new_process = Process(
            target=worker_func,
            kwargs={"args": args},
            name=f"worker-{self.worker_num}",
            daemon=True,
        )
        new_process.start()
        logger.info(f"Process {new_process.name} restarted with pid {new_process.pid}")
        workers[self.worker_num] = new_process


@dataclass
class ShutdownAction(ProcessActionBase):
    """This action shuts down process manager loop."""


def schedule_workers_reload(
    action_queue: "Queue[ProcessActionBase]",
) -> None:
    """
    Function to schedule workers to restart.

    It simply send FULL_RELOAD event, which is handled
    in the mainloop.

    :param action_queue: queue to send events to.
    """
    action_queue.put(ReloadAllAction())
    logger.info("Scheduled workers reload.")


def get_signal_handler(
    action_queue: "Queue[ProcessActionBase]",
) -> Callable[[int, Any], None]:
    """
    Generate singnal handler for main process.

    The signal handler will just put the SHUTDOWN event in
    the action queue.

    :param action_queue: event queue.
    :returns: actual signal handler.
    """

    def _signal_handler(signum: int, _frame: Any) -> None:
        logger.debug(f"Got signal {signum}.")
        action_queue.put(ShutdownAction())
        logger.warn("Workers are scheduled for shutdown.")

    return _signal_handler


class ProcessManager:
    """
    Process manager for taskiq.

    This class spawns multiple processes,
    and maintains their states. If process
    is down, it tries to restart it.
    """

    def __init__(
        self,
        args: WorkerArgs,
        observer: Observer,
        worker_function: Callable[[WorkerArgs], None],
    ) -> None:
        self.worker_function = worker_function
        self.action_queue: "Queue[ProcessActionBase]" = Queue(-1)
        self.args = args
        if args.reload:
            observer.schedule(
                FileWatcher(
                    callback=schedule_workers_reload,
                    use_gitignore=not args.no_gitignore,
                    action_queue=self.action_queue,
                ),
                path=".",
                recursive=True,
            )

        signal_handler = get_signal_handler(self.action_queue)
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        self.workers: List[Process] = []

    def prepare_workers(self) -> None:
        """Spawn multiple processes."""
        for process in range(self.args.workers):
            work_proc = Process(
                target=self.worker_function,
                kwargs={"args": self.args},
                name=f"worker-{process}",
                daemon=True,
            )
            work_proc.start()
            logger.info(
                "Started process worker-%d with pid %s ",
                process,
                work_proc.pid,
            )
            self.workers.append(work_proc)

    def start(self) -> None:  # noqa: C901, WPS213
        """
        Start managing child processes.

        This function is an endless loop,
        which listens to new events from different sources.

        Every second it checks for new events and
        current states of child processes.

        If there are new events it handles them.
        Manager can handle 3 types of events:

        1. `ReloadAllAction` - when we want to restart all child processes.
            It checks for running processes and generates RELOAD_ONE event for
            any process.

        2. `ReloadOneAction` - this event restarts one single child process.

        3. `ShutdownAction` - exits the loop. Since all child processes are
            daemons, they will be automatically terminated using signals.

        After all events are handled, it iterates over all child processes and
        checks that all processes are healthy. If process was terminated for
        some reason, it schedules a restart for dead process.
        """
        self.prepare_workers()
        while True:
            sleep(1)
            reloaded_workers = set()
            # We bulk_process all pending events.
            while not self.action_queue.empty():
                action = self.action_queue.get()
                logging.debug(f"Got event: {action}")
                if isinstance(action, ReloadAllAction):
                    action.handle(
                        workers_num=len(self.workers),
                        action_queue=self.action_queue,
                    )
                elif isinstance(action, ReloadOneAction):
                    # If we just reloaded this worker, skip handling.
                    if action.worker_num in reloaded_workers:
                        continue
                    action.handle(self.workers, self.args, self.worker_function)
                    reloaded_workers.add(action.worker_num)
                elif isinstance(action, ShutdownAction):
                    logger.debug("Process manager closed.")
                    return

            for worker_num, worker in enumerate(self.workers):
                if not worker.is_alive():
                    logger.info(f"{worker.name} is dead. Scheduling reload.")
                    self.action_queue.put(ReloadOneAction(worker_num=worker_num))
