import asyncio
import logging
import os
import signal
import sys
from contextlib import suppress
from dataclasses import dataclass
from multiprocessing import Event, Process, Queue, current_process
from multiprocessing.synchronize import Event as EventType
from pathlib import Path
from time import sleep
from typing import Any, Callable, List, Optional

try:
    from watchdog.observers import Observer

    from taskiq.cli.watcher import FileWatcher
except ImportError:
    Observer = None  # type: ignore
    FileWatcher = None  # type: ignore

from taskiq.cli.worker.args import WorkerArgs

try:
    from taskiq.health.heartbeat import WorkerHeartbeatArray
    from taskiq.health.server import HealthCheckServer

    health_available = True
except ImportError:
    WorkerHeartbeatArray = None  # type: ignore[assignment, misc]
    HealthCheckServer = None  # type: ignore[assignment, misc]
    health_available = False

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
            action_queue.put(ReloadOneAction(worker_num=worker_id, is_reload_all=True))


@dataclass
class ReloadOneAction(ProcessActionBase):
    """This action reloads single worker with particular id."""

    worker_num: int
    is_reload_all: bool

    def handle(
        self,
        workers: List[Process],
        args: WorkerArgs,
        worker_func: Callable[[WorkerArgs], None],
        heartbeat_array: Optional[Any] = None,
    ) -> None:
        """
        This action reloads a single process.

        :param workers: known children processes.
        :param args: args for new process.
        :param worker_func: function that is used to start worker processes.
        :param heartbeat_array: optional heartbeat array for health checks.
        """
        if self.worker_num < 0 or self.worker_num >= len(workers):
            logger.warning("Unknown worker id.")
            return
        worker = workers[self.worker_num]
        try:
            worker.terminate()
        except ValueError:
            logger.debug("Process %s is already terminated.", worker.name)
        # Waiting worker shutdown.
        worker.join()
        event: EventType = Event()

        # Create wrapper function if health checks enabled
        if heartbeat_array is not None:

            def make_worker_wrapper(
                worker_id: int,
                heartbeat_array: Any,
            ) -> Callable[[], None]:
                def _wrapper() -> None:
                    from taskiq.cli.worker.run import start_listen

                    start_listen(args, worker_id, heartbeat_array)

                return _wrapper

            worker_wrapper = make_worker_wrapper(self.worker_num, heartbeat_array)

            new_process = Process(
                target=worker_wrapper,
                name=f"worker-{self.worker_num}",
                daemon=False,
            )
        else:
            new_process = Process(
                target=worker_func,
                kwargs={"args": args},
                name=f"worker-{self.worker_num}",
                daemon=False,
            )
        new_process.start()
        logger.info(
            "Process %s restarted with pid %s",
            new_process.name,
            new_process.pid,
        )
        workers[self.worker_num] = new_process
        _wait_for_worker_startup(new_process, event)


@dataclass
class ShutdownAction(ProcessActionBase):
    """This action shuts down process manager loop."""


def _wait_for_worker_startup(process: Process, event: EventType) -> None:
    """Wait for worker process to start up.

    Args:
        process: The worker process to wait for
        event: Event that signals worker startup (currently unused)
    """
    while process.is_alive():
        with suppress(TimeoutError):
            event.wait(0.1)
            return


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
    action_to_send: ProcessActionBase,
) -> Callable[[int, Any], None]:
    """
    Generate signal handler for main process.

    The signal handler will just put the SHUTDOWN event in
    the action queue.

    :param action_queue: event queue.
    :param action_to_send: action that will be sent to the queue on signal.
    :returns: actual signal handler.
    """

    def _signal_handler(signum: int, _frame: Any) -> None:
        if current_process().name.startswith("worker"):
            raise KeyboardInterrupt

        logger.debug("Got signal %d.", signum)
        action_queue.put(action_to_send)
        logger.warning("Workers are scheduled for shutdown.")

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
        worker_function: Callable[[WorkerArgs], None],
        observer: Optional[Observer] = None,  # type: ignore[valid-type]
    ) -> None:
        self.worker_function = worker_function
        self.action_queue: "Queue[ProcessActionBase]" = Queue(-1)
        self.args = args

        # Initialize heartbeat system if health checks enabled
        self.heartbeat_array: Optional[Any] = None
        self.health_server: Optional[Any] = None
        self._health_server_task: Optional[asyncio.Task[None]] = None

        if args.health_check_enable and health_available:
            self.heartbeat_array = WorkerHeartbeatArray(args.workers)  # type: ignore[misc]
            self.health_server = HealthCheckServer(  # type: ignore[misc]
                port=args.health_check_port,
                heartbeat_array=self.heartbeat_array,
                timeout=args.health_check_timeout,
                host=args.health_check_host,
            )
        elif args.health_check_enable and not health_available:
            logger.warning(
                "Health checks requested but health module not available",
            )
        if args.reload and observer is not None:
            watch_paths = args.reload_dirs if args.reload_dirs else ["."]
            for path_to_watch in watch_paths:
                logger.debug("Watching directory: %s", path_to_watch)
                observer.schedule(
                    FileWatcher(
                        callback=schedule_workers_reload,
                        path=Path(path_to_watch),
                        use_gitignore=not args.no_gitignore,
                        action_queue=self.action_queue,
                    ),
                    path=path_to_watch,
                    recursive=True,
                )

        shutdown_handler = get_signal_handler(self.action_queue, ShutdownAction())
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        if sys.platform != "win32":
            signal.signal(
                signal.SIGHUP,
                get_signal_handler(self.action_queue, ReloadAllAction()),
            )

        self.workers: List[Process] = []

    async def start_health_server(self) -> None:
        """Start health check server in main process."""
        if self.health_server:
            await self.health_server.start()

    async def stop_health_server(self) -> None:
        """Stop health check server."""
        if self.health_server:
            await self.health_server.stop()

    def prepare_workers(self) -> None:
        """Spawn multiple processes."""
        events: List[EventType] = []
        for process in range(self.args.workers):
            event = Event()

            # Create wrapper function if health checks enabled
            if self.heartbeat_array is not None:
                # Use a factory function to avoid closure issues
                def make_worker_wrapper(
                    worker_id: int,
                    heartbeat_array: Any,
                ) -> Callable[[], None]:
                    def _wrapper() -> None:
                        # Import start_listen locally to avoid circular imports
                        from taskiq.cli.worker.run import start_listen

                        start_listen(self.args, worker_id, heartbeat_array)

                    return _wrapper

                worker_wrapper = make_worker_wrapper(process, self.heartbeat_array)

                work_proc = Process(
                    target=worker_wrapper,
                    name=f"worker-{process}",
                    daemon=False,
                )
            else:
                # Normal case without health checks
                work_proc = Process(
                    target=self.worker_function,
                    kwargs={"args": self.args},
                    name=f"worker-{process}",
                    daemon=False,
                )
            work_proc.start()
            logger.info(
                "Started process worker-%d with pid %s ",
                process,
                work_proc.pid,
            )
            self.workers.append(work_proc)
            events.append(event)

        # Wait for workers startup
        for worker, event in zip(self.workers, events):
            _wait_for_worker_startup(worker, event)

    def _start_health_server_if_needed(self) -> None:
        """Start health server in background thread if enabled."""
        if self.health_server:
            import asyncio
            import threading

            def run_health_server() -> None:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    # We know health_server is not None due to outer check
                    if self.health_server is None:
                        raise RuntimeError("Health server is unexpectedly None")
                    loop.run_until_complete(self.health_server.start())
                    # Keep the server running
                    loop.run_forever()
                except Exception as exc:
                    logger.error("Health server error: %s", exc)
                finally:
                    loop.close()

            health_thread = threading.Thread(target=run_health_server, daemon=True)
            health_thread.start()
            logger.info("Health check server started in background thread")

    def _stop_health_server_if_needed(self) -> None:
        """Stop health server if running."""
        if self.health_server:
            # The server will be stopped when the daemon thread exits
            logger.info("Health check server will stop with main process")

    def start(self) -> Optional[int]:  # noqa: C901
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

        :returns: status code or None.
        """
        restarts = 0
        self.prepare_workers()

        # Start health server if enabled
        self._start_health_server_if_needed()
        while True:
            sleep(1)
            reloaded_workers = set()
            # We bulk_process all pending events.
            while not self.action_queue.empty():
                action = self.action_queue.get()
                logging.debug("Got event: %s", action)
                if isinstance(action, ReloadAllAction):
                    action.handle(
                        workers_num=len(self.workers),
                        action_queue=self.action_queue,
                    )
                elif isinstance(action, ReloadOneAction):
                    # We check if max_fails is set.
                    # If it's true, we check how many times
                    # worker was reloaded.
                    if not action.is_reload_all and self.args.max_fails >= 1:
                        restarts += 1
                        if restarts >= self.args.max_fails:
                            logger.warning("Max restarts reached. Exiting.")
                            # Returning error status.
                            return -1
                    # If we just reloaded this worker, skip handling.
                    if action.worker_num in reloaded_workers:
                        continue
                    action.handle(
                        self.workers,
                        self.args,
                        self.worker_function,
                        self.heartbeat_array,
                    )
                    reloaded_workers.add(action.worker_num)
                elif isinstance(action, ShutdownAction):
                    logger.debug("Process manager closed, killing workers.")
                    for worker in self.workers:
                        if worker.pid:
                            os.kill(worker.pid, signal.SIGINT)
                    self._stop_health_server_if_needed()
                    return None

            for worker_num, worker in enumerate(self.workers):
                if not worker.is_alive():
                    logger.info("%s is dead. Scheduling reload.", worker.name)
                    self.action_queue.put(
                        ReloadOneAction(
                            worker_num=worker_num,
                            is_reload_all=False,
                        ),
                    )
