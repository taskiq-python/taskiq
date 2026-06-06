"""Declare shared task definitions and bind them in the final application."""

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from taskiq import (
    AsyncTaskiqDecoratedTask,
    Flow,
    InMemoryBroker,
    TaskiqRouter,
    task_builder,
)


@dataclass(frozen=True, slots=True)
class BillingQueue:
    """Broker-specific flow that follows the shared flow protocol."""

    name: str
    priority: int

    def broker_options(self) -> Mapping[str, object]:
        """Return options that a billing broker adapter can understand."""
        return {
            "priority": self.priority,
        }


class BillingTask(AsyncTaskiqDecoratedTask[Any, Any]):
    """Custom task class shared by billing package tasks."""

    def billing_name(self) -> str:
        """Return a billing-specific task name."""
        return self.task_name


@task_builder("billing.calculate_total", base_cls=BillingTask, domain="billing")
async def calculate_total(price: int, quantity: int) -> int:
    """Package-level task definition that is not bound to any broker."""
    return price * quantity


router = TaskiqRouter()
billing_flow = Flow("billing.tasks")
priority_billing_flow = BillingQueue(name="billing.priority", priority=10)

billing_broker = InMemoryBroker(
    router=router,
    broker_name="billing",
    default_flow=billing_flow,
    await_inplace=True,
)

registered_calculate_total = router.register_task(
    calculate_total,
    broker=billing_broker,
    flow=billing_flow,
)
router.subscribe(
    billing_broker,
    billing_flow,
    registered_calculate_total,
)


async def _main() -> None:
    await billing_broker.startup()
    try:
        direct_result = await calculate_total.call(19, 3)

        priority_route = router.resolve_route(
            registered_calculate_total,
            broker=billing_broker,
            flow=priority_billing_flow,
        )
        prepared_task = (
            registered_calculate_total.kicker()
            .with_route(
                priority_route,
            )
            .prepare(19, 3)
        )

        queued_task = await prepared_task.kiq()
        queued_result = await queued_task.wait_result(timeout=2)

        print(f"Shared task direct call: {direct_result}")
        print(f"Registered task class: {registered_calculate_total.billing_name()}")
        listen_flow = billing_broker.get_subscribed_flows()[0]
        print(f"Registered listen flow: {listen_flow.name}")
        print(f"Prepared message: {prepared_task.message.task_name}")
        print(f"Registered queued call: {queued_result.return_value}")
    finally:
        await billing_broker.shutdown()


if __name__ == "__main__":
    asyncio.run(_main())
