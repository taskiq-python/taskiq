import multiprocessing
import queue
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

BROKER_MODULE = """
import asyncio
from collections.abc import AsyncGenerator

from taskiq import AsyncBroker, BrokerMessage


class IdleBroker(AsyncBroker):
    async def kick(self, message: BrokerMessage) -> None:
        pass

    async def listen(self) -> AsyncGenerator[bytes, None]:
        while True:
            await asyncio.sleep(3600)
            yield b""


broker = IdleBroker()
"""

# Runs the taskiq CLI, optionally forcing a multiprocessing start method first.
CLI_RUNNER = """
import multiprocessing
import sys

from taskiq.__main__ import main

if __name__ == "__main__":
    start_method = sys.argv.pop(1)
    if start_method != "default":
        multiprocessing.set_start_method(start_method)
    main()
"""

STARTUP_TIMEOUT = 30


def worker_start_methods() -> list[str]:
    # The worker always uses "spawn" on macOS: forcing another method would conflict.
    if sys.platform == "darwin":
        return ["default"]
    # The default method is one of these, so it's covered without a separate run.
    return multiprocessing.get_all_start_methods()


def wait_for_output(process: subprocess.Popen[str], text: str) -> bool:
    lines: queue.Queue[str] = queue.Queue()

    def read_output() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            lines.put(line)

    threading.Thread(target=read_output, daemon=True).start()
    deadline = time.monotonic() + STARTUP_TIMEOUT
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            if text in lines.get(timeout=remaining):
                return True
        except queue.Empty:
            return False
    return False


def stop(process: subprocess.Popen[str]) -> None:
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Worker processes can't be stopped with SIGINT on Windows.",
)
@pytest.mark.parametrize("start_method", worker_start_methods())
def test_worker_process_logs_listening_started(
    tmp_path: Path,
    start_method: str,
) -> None:
    """
    Worker processes emit their own log records.

    A worker started with "spawn" or "forkserver" (the default on Linux
    since Python 3.14) doesn't inherit the main process logging config.
    """
    (tmp_path / "idle_broker.py").write_text(BROKER_MODULE)
    (tmp_path / "run_cli.py").write_text(CLI_RUNNER)
    process = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "run_cli.py",
            start_method,
            "worker",
            "idle_broker:broker",
            "--workers",
            "1",
            "--log-level",
            "INFO",
        ],
        cwd=tmp_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        assert wait_for_output(process, "Listening started.")
    finally:
        stop(process)
