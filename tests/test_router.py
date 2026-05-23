from collections.abc import AsyncGenerator, Mapping
from dataclasses import dataclass

import pytest

from taskiq import Flow, FlowProtocol, TaskiqRouter, task_builder
from taskiq.abc.broker import AsyncBroker
from taskiq.message import BrokerMessage


@dataclass(frozen=True, slots=True)
class BrokerQueue:
    """Broker-specific flow used to prove protocol-based routing."""

    name: str
    durable: bool = True

    def broker_options(self, broker_name: str) -> Mapping[str, object]:
        """Return options for the target broker."""
        return {
            "broker": broker_name,
            "durable": self.durable,
        }


class RecordingBroker(AsyncBroker):
    """Broker that records sent messages and flows."""

    def __init__(
        self,
        *,
        router: TaskiqRouter | None = None,
        broker_name: str | None = None,
        default_flow: FlowProtocol | None = None,
    ) -> None:
        self.sent: list[tuple[BrokerMessage, FlowProtocol | None]] = []
        super().__init__(
            router=router,
            broker_name=broker_name,
            default_flow=default_flow,
        )

    async def kick(self, message: BrokerMessage) -> None:
        """Record old-style send."""
        self.sent.append((message, None))

    async def kick_to_flow(
        self,
        message: BrokerMessage,
        flow: FlowProtocol | None = None,
    ) -> None:
        """Record flow-aware send."""
        self.sent.append((message, flow))

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """Recording broker doesn't listen in these tests."""
        if False:
            yield b""


def test_broker_creates_default_router() -> None:
    broker = RecordingBroker()

    assert broker.router.brokers[broker.broker_name] is broker
    assert broker.router.default_broker_name == broker.broker_name


async def test_old_broker_task_api_registers_task_in_router() -> None:
    broker = RecordingBroker()

    @broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    assert broker.find_task("demo.task") is demo_task
    assert broker.router.find_task("demo.task") is demo_task

    await demo_task.kiq()

    assert broker.sent[0][0].task_name == "demo.task"
    assert broker.sent[0][1] is None


async def test_router_can_route_task_to_another_broker_flow() -> None:
    router = TaskiqRouter()
    source = RecordingBroker(router=router, broker_name="source")
    target = RecordingBroker(router=router, broker_name="target")
    flow = Flow("events")

    @source.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    route = router.route_task(demo_task, broker=target, flow=flow)

    await demo_task.kiq()

    assert route.broker is target
    assert route.broker_name == "target"
    assert source.sent == []
    assert target.sent[0][0].task_name == "demo.task"
    assert target.sent[0][1] == flow


async def test_kicker_route_override_wins_over_registered_route() -> None:
    router = TaskiqRouter()
    first = RecordingBroker(router=router, broker_name="first")
    second = RecordingBroker(router=router, broker_name="second")
    first_flow = Flow("first")
    second_flow = Flow("second")

    @first.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    router.route_task(demo_task, broker=first, flow=first_flow)

    await demo_task.kicker().with_route(second, second_flow).kiq()

    assert first.sent == []
    assert second.sent[0][1] == second_flow


async def test_router_keeps_string_broker_lookup_for_compatibility() -> None:
    router = TaskiqRouter()
    source = RecordingBroker(router=router, broker_name="source")
    target = RecordingBroker(router=router, broker_name="target")
    flow = Flow("compat")

    @source.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    router.route_task("demo.task", broker="target", flow=flow)

    await demo_task.kicker().with_route("target", flow).kiq()

    assert target.sent[0][0].task_name == "demo.task"
    assert target.sent[0][1] == flow


async def test_router_accepts_broker_specific_flow_protocol() -> None:
    broker = RecordingBroker(broker_name="transport")
    flow = BrokerQueue(name="critical", durable=False)

    @broker.task(task_name="demo.task")
    async def demo_task() -> None:
        return None

    await demo_task.kicker().with_flow(flow).kiq()

    assert isinstance(flow, FlowProtocol)
    assert broker.sent[0][1] is flow
    assert flow.broker_options("transport") == {
        "broker": "transport",
        "durable": False,
    }


async def test_kicker_can_prepare_invocation_for_later() -> None:
    broker = RecordingBroker()

    @broker.task(task_name="demo.task")
    async def demo_task(value: int) -> None:
        return None

    prepared = demo_task.kicker().with_labels(trace_id="abc").prepare(1)

    assert prepared.message.task_name == "demo.task"
    assert prepared.message.args == [1]
    assert prepared.message.labels["trace_id"] == "abc"

    await prepared.kiq()

    assert broker.sent[0][0].task_id == prepared.message.task_id


async def test_task_builder_can_be_registered_later() -> None:
    broker = RecordingBroker()

    @task_builder("shared.add", queue="shared")
    def add(left: int, right: int) -> int:
        return left + right

    assert await add.call(1, 2) == 3

    registered = broker.register_task(add)

    assert registered.task_name == "shared.add"
    assert registered.labels == {"queue": "shared"}
    assert broker.router.find_task("shared.add") is registered

    await registered.kiq(1, 2)

    assert broker.sent[0][0].task_name == "shared.add"


async def test_router_task_decorator_can_choose_broker_and_flow() -> None:
    router = TaskiqRouter()
    target = RecordingBroker(router=router, broker_name="target")
    flow = Flow("target-flow")

    @router.task("demo.task", broker=target, flow=flow)
    async def demo_task() -> None:
        return None

    await demo_task.kiq()

    assert target.sent[0][0].task_name == "demo.task"
    assert target.sent[0][1] == flow


def test_router_rejects_duplicate_broker_names() -> None:
    router = TaskiqRouter()
    RecordingBroker(router=router, broker_name="broker")

    with pytest.raises(ValueError, match="already registered"):
        RecordingBroker(router=router, broker_name="broker")
