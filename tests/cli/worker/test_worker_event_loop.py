import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
import zmq

from taskiq import InMemoryBroker
from taskiq.message import TaskiqMessage

ZMQ_APP = """
from taskiq import ZeroMQBroker

broker = ZeroMQBroker(zmq_sub_host="tcp://127.0.0.1:{port}")


@broker.task(task_name="ping")
async def ping() -> None:
    print("task executed", flush=True)
"""

OTHER_BROKER_APP = """
import asyncio

from taskiq import AsyncBroker


class ReportingBroker(AsyncBroker):
    async def kick(self, message):
        pass

    async def listen(self):
        print(type(asyncio.get_running_loop()).__name__, flush=True)
        return
        yield


broker = ReportingBroker()
"""

WORKER_RUNNER = """
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.run import start_listen

start_listen(WorkerArgs(broker="app:broker", modules=[]))
"""

WORKER_TIMEOUT = 30


def test_zmq_worker_executes_published_task(tmp_path: Path) -> None:
    """
    A worker with ZeroMQBroker receives and executes tasks.

    The default event loop on Windows can't wait for ZeroMQ sockets.
    """
    message = InMemoryBroker().formatter.dumps(
        TaskiqMessage(
            task_id="1",
            task_name="ping",
            labels={},
            args=[],
            kwargs={},
        ),
    )
    with zmq.Context() as context, context.socket(zmq.PUB) as publisher:
        publisher.setsockopt(zmq.LINGER, 0)
        port = publisher.bind_to_random_port("tcp://127.0.0.1")
        (tmp_path / "app.py").write_text(ZMQ_APP.format(port=port))
        (tmp_path / "run_worker.py").write_text(WORKER_RUNNER)
        process = subprocess.Popen(
            [sys.executable, "run_worker.py"],
            cwd=tmp_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output: list[str] = []

        def read_output() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                output.append(line)

        reader = threading.Thread(target=read_output, daemon=True)
        reader.start()
        try:
            deadline = time.monotonic() + WORKER_TIMEOUT
            # A subscriber misses everything published before it connects.
            while (
                time.monotonic() < deadline
                and process.poll() is None
                and not any("task executed" in line for line in output)
            ):
                publisher.send(message.message)
                time.sleep(0.1)
        finally:
            process.kill()
            process.wait()
            reader.join()

    assert any("task executed" in line for line in output), "".join(output)


@pytest.mark.skipif(sys.platform != "win32", reason="Only Windows has Proactor loop.")
def test_other_brokers_keep_default_loop_on_windows(tmp_path: Path) -> None:
    """Only ZeroMQBroker replaces the loop, others may need subprocess support."""
    (tmp_path / "app.py").write_text(OTHER_BROKER_APP)
    (tmp_path / "run_worker.py").write_text(WORKER_RUNNER)

    result = subprocess.run(
        [sys.executable, "run_worker.py"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=WORKER_TIMEOUT,
        check=False,
    )

    assert "ProactorEventLoop" in result.stdout, result.stdout + result.stderr
