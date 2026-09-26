import asyncio
from unittest.mock import patch

from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.run import _create_worker_event_loop, start_listen


def test_create_worker_event_loop_uses_configured_factory() -> None:
    args = WorkerArgs(
        broker="example:broker",
        modules=[],
        loop_factory="asyncio:SelectorEventLoop",
    )

    with patch("taskiq.cli.worker.run.uvloop") as uvloop:
        loop = _create_worker_event_loop(args)

    try:
        assert isinstance(loop, asyncio.SelectorEventLoop)
        uvloop.new_event_loop.assert_not_called()
    finally:
        loop.close()


def test_create_worker_event_loop_uses_uvloop_by_default() -> None:
    args = WorkerArgs(broker="example:broker", modules=[])
    expected_loop = asyncio.new_event_loop()

    with patch("taskiq.cli.worker.run.uvloop") as uvloop:
        uvloop.new_event_loop.return_value = expected_loop
        loop = _create_worker_event_loop(args)

    try:
        assert loop is expected_loop
        uvloop.new_event_loop.assert_called_once_with()
    finally:
        loop.close()


def test_create_worker_event_loop_uses_asyncio_without_uvloop() -> None:
    args = WorkerArgs(broker="example:broker", modules=[])

    with patch("taskiq.cli.worker.run.uvloop", new=None):
        loop = _create_worker_event_loop(args)

    try:
        assert isinstance(loop, asyncio.AbstractEventLoop)
    finally:
        loop.close()


def test_start_listen_uses_created_event_loop() -> None:
    args = WorkerArgs(broker="example:broker", modules=[])
    broker = InMemoryBroker()
    loop = asyncio.new_event_loop()

    class Receiver:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def listen(self, _shutdown_event: asyncio.Event) -> None:
            pass

    with (
        patch("taskiq.cli.worker.run.signal.signal"),
        patch("taskiq.cli.worker.run._create_worker_event_loop", return_value=loop),
        patch("taskiq.cli.worker.run.import_object", return_value=broker),
        patch("taskiq.cli.worker.run.import_tasks"),
        patch("taskiq.cli.worker.run.get_receiver_type", return_value=Receiver),
    ):
        start_listen(args)

    try:
        assert loop.is_closed() is False
    finally:
        asyncio.set_event_loop(None)
        loop.close()
