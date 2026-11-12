"""Integration test for --hardkill-count signal semantics.

Spawns a real worker subprocess whose broker never finishes graceful
shutdown, sends SIGINTs directly to a worker child process (the process
that installs the hard-kill counting signal handler), and checks how
many signals it takes to hard-kill it.

Documented behavior (``--hardkill-count N``): N termination signals are
allowed before performing a hardkill, i.e. signals 1..N only set the
shutdown event and signal N+1 raises KeyboardInterrupt in the worker.
"""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import psutil
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SIGNAL_SETTLE_SECONDS = 0.6
WAIT_TIMEOUT = 15.0


def _pid_alive(pid: int) -> bool:
    """Check whether a process exists."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _worker_child_pids(main_pid: int) -> list[int]:
    """Return pids of worker child processes of the main process."""
    try:
        parent = psutil.Process(main_pid)
    except psutil.NoSuchProcess:
        return []
    return [child.pid for child in parent.children(recursive=False)]


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="SIGINT hard-kill semantics differ on Windows",
)
def test_hardkill_count_signals_before_hardkill() -> None:
    """N signals are graceful; signal N+1 hard-kills the worker process."""
    hardkill_count = 3
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-m",
            "taskiq",
            "worker",
            "tests.cli.worker.hanging_broker:broker",
            "--hardkill-count",
            str(hardkill_count),
            "--workers",
            "1",
            "--no-configure-logging",
            # Keep graceful shutdown pending so only the hard-kill
            # path can terminate the worker during the test.
            "--shutdown-timeout",
            "3600",
        ],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        # Wait for the worker child to spawn and install its handlers.
        deadline = time.monotonic() + WAIT_TIMEOUT
        worker_pid: int | None = None
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                pytest.fail(
                    f"Worker exited during startup: returncode={proc.returncode}",
                )
            children = _worker_child_pids(proc.pid)
            if children:
                worker_pid = children[0]
                break
            time.sleep(0.25)
        if worker_pid is None:
            pytest.fail("No worker child process spawned in time")
        # Extra settle time so the child's signal handlers are installed.
        time.sleep(2.0)

        # Signals 1..N: only set the shutdown event; the broker's graceful
        # shutdown hangs, so the worker process must still be alive.
        for signal_number in range(1, hardkill_count + 1):
            os.kill(worker_pid, signal.SIGINT)
            time.sleep(SIGNAL_SETTLE_SECONDS)
            assert _pid_alive(worker_pid), (
                f"Worker exited on signal {signal_number} of "
                f"{hardkill_count} allowed graceful signals"
            )

        # Signal N+1: hard kill raises KeyboardInterrupt in the worker.
        os.kill(worker_pid, signal.SIGINT)
        deadline = time.monotonic() + WAIT_TIMEOUT
        while time.monotonic() < deadline and _pid_alive(worker_pid):
            time.sleep(0.1)
        assert not _pid_alive(worker_pid), (
            f"Worker still alive after {hardkill_count + 1} signals; "
            f"--hardkill-count {hardkill_count} should hard-kill on "
            f"signal {hardkill_count + 1}"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
