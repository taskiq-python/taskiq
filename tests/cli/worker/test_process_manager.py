import logging
import signal

import pytest

from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.process_manager import ProcessManager


def exit_immediately(args: WorkerArgs) -> None:
    pass


def test_shutdown_workers_skips_exited_worker(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    A worker that has already exited doesn't break the shutdown.

    On Windows `os.kill` raises `PermissionError` for such a process.
    """
    # ProcessManager must not replace signal handlers of the pytest process.
    monkeypatch.setattr(signal, "signal", lambda *_: None)
    manager = ProcessManager(
        args=WorkerArgs(broker="", modules=[], workers=1),
        worker_function=exit_immediately,
    )
    manager.prepare_workers()
    manager.workers[0].join()

    with caplog.at_level(logging.INFO, logger="taskiq.process-manager"):
        manager._shutdown_workers()

    assert "already terminated" in caplog.text
