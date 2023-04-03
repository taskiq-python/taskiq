import asyncio
from contextlib import contextmanager
from typing import Generator

import pytest

from taskiq.abc.broker import AsyncBroker
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.async_task_runner import async_listen_messages


@contextmanager
def receive_messages(broker: AsyncBroker) -> Generator[None, None, None]:
    cli_args = WorkerArgs(broker="", modules=[])
    listen_task = asyncio.create_task(async_listen_messages(broker, cli_args))
    yield
    listen_task.cancel()


@pytest.mark.anyio
async def test_inmemory_broker_handle_message() -> None:
    """Test that inmemory broker receive and handle task works."""
    broker = InMemoryBroker()

    @broker.task()
    async def test_echo(msg: str) -> str:
        return msg

    with receive_messages(broker):
        task = await test_echo.kiq("foo")
        result = await task.wait_result(timeout=1)
    assert result.return_value == "foo"
