import asyncio

import pytest

from taskiq.brokers import zmq_broker
from taskiq.events import TaskiqEvents
from taskiq.state import TaskiqState

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(zmq_broker.zmq is None, reason="pyzmq is not installed"),
]


@pytest.mark.parametrize("is_worker", [False, True])
@pytest.mark.parametrize("failure_stage", [None, "handler", "backend"])
async def test_shutdown_releases_zmq_resources(
    monkeypatch: pytest.MonkeyPatch,
    is_worker: bool,
    failure_stage: str | None,
) -> None:
    broker = zmq_broker.ZeroMQBroker(zmq_pub_host="inproc://taskiq-shutdown-test")
    broker.is_worker_process = is_worker
    event = TaskiqEvents.WORKER_SHUTDOWN if is_worker else TaskiqEvents.CLIENT_SHUTDOWN
    called = []

    async def shutdown_handler(state: TaskiqState) -> None:
        called.append("handler")
        if failure_stage == "handler":
            raise RuntimeError("shutdown handler failed")

    async def shutdown_backend() -> None:
        called.append("backend")
        if failure_stage == "backend":
            raise RuntimeError("shutdown backend failed")

    broker.event_handlers[event].append(shutdown_handler)
    monkeypatch.setattr(broker.result_backend, "shutdown", shutdown_backend)

    try:
        await broker.startup()
        assert not broker.socket.closed
        assert not broker.context.closed

        if failure_stage is None:
            await broker.shutdown()
        else:
            with pytest.raises(RuntimeError, match=f"shutdown {failure_stage} failed"):
                await broker.shutdown()

        assert called == (
            ["handler"] if failure_stage == "handler" else ["handler", "backend"]
        )
        assert broker.socket.closed
        assert broker.context.closed
    finally:
        broker.context.destroy(linger=0)


@pytest.mark.parametrize("is_worker", [False, True])
async def test_zmq_broker_can_restart(is_worker: bool) -> None:
    broker = zmq_broker.ZeroMQBroker(zmq_pub_host="inproc://taskiq-restart-test")
    broker.is_worker_process = is_worker

    try:
        for _ in range(2):
            await broker.startup()
            assert not broker.socket.closed
            assert not broker.context.closed
            await broker.shutdown()
            assert broker.socket.closed
            assert broker.context.closed
    finally:
        broker.context.destroy(linger=0)


async def test_shutdown_closes_suspended_listener() -> None:
    broker = zmq_broker.ZeroMQBroker(
        zmq_sub_host="inproc://taskiq-listener-shutdown-test",
    )
    broker.is_worker_process = True
    await broker.startup()
    listener = broker.listen()
    pending = asyncio.create_task(anext(listener))

    try:
        await asyncio.sleep(0)
        assert not pending.done()

        await broker.shutdown()

        with pytest.raises(asyncio.CancelledError):
            await pending
        await listener.aclose()
        assert broker.socket.closed
        assert broker.context.closed
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await listener.aclose()
        broker.context.destroy(linger=0)
