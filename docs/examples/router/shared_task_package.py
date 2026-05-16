"""Declare shared task definitions and bind them in the final application."""

import asyncio

from taskiq import Flow, InMemoryBroker, TaskiqRouter, task_builder


@task_builder("billing.calculate_total", domain="billing")
async def calculate_total(price: int, quantity: int) -> int:
    """Package-level task definition that is not bound to any broker."""
    return price * quantity


router = TaskiqRouter()
billing_flow = Flow.queue("billing.tasks")
priority_billing_flow = Flow.queue("billing.priority")

billing_broker = InMemoryBroker(
    router=router,
    broker_name="billing",
    default_flow=billing_flow,
    await_inplace=True,
)

registered_calculate_total = billing_broker.register_task(calculate_total)


async def _main() -> None:
    await billing_broker.startup()
    try:
        direct_result = await calculate_total.call(19, 3)

        prepared_task = registered_calculate_total.kicker().with_flow(
            priority_billing_flow,
        ).prepare(19, 3)

        queued_task = await prepared_task.kiq()
        queued_result = await queued_task.wait_result(timeout=2)

        print(f"Shared task direct call: {direct_result}")
        print(f"Prepared message: {prepared_task.message.task_name}")
        print(f"Registered queued call: {queued_result.return_value}")
    finally:
        await billing_broker.shutdown()


if __name__ == "__main__":
    asyncio.run(_main())
