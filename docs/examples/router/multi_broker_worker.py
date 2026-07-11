"""Expose an explicit multi-broker listener tuple for the worker CLI."""

import asyncio
from collections.abc import AsyncGenerator

from taskiq import AsyncBroker, BrokerMessage, Flow, TaskiqRouter


class DemoBroker(AsyncBroker):
    """Small in-process transport used to keep this configuration executable."""

    def __init__(self, router: TaskiqRouter, broker_name: str) -> None:
        super().__init__(router=router, broker_name=broker_name)
        self.messages: asyncio.Queue[bytes] = asyncio.Queue()

    async def kick(self, message: BrokerMessage) -> None:
        await self.messages.put(message.message)

    async def listen(self) -> AsyncGenerator[bytes, None]:
        while True:
            yield await self.messages.get()


router = TaskiqRouter()

commands_broker = DemoBroker(router, "commands")
events_broker = DemoBroker(router, "events")
outbound_broker = DemoBroker(router, "outbound")


@commands_broker.task(task_name="orders.notify")
async def notify_order(order_id: str) -> str:
    return f"notified:{order_id}"


@commands_broker.task(task_name="orders.process")
async def process_order(order_id: str) -> str:
    await notify_order.kiq(order_id)
    return f"processed:{order_id}"


orders_flow = Flow("orders.events")
notifications_flow = Flow("orders.notifications")
router.route_task(process_order, broker=events_broker, flow=orders_flow)
router.route_task(
    notify_order,
    broker=outbound_broker,
    flow=notifications_flow,
)
router.subscribe(events_broker, orders_flow, process_order)

# Only these brokers receive listeners. The outbound broker belongs to the
# same Router, so this explicit multi-broker worker starts its client lifecycle
# for routed child sends without consuming from it.
worker_brokers = (commands_broker, events_broker)
