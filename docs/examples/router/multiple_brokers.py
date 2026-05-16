"""Route one task through several brokers with a shared router."""

import asyncio

from taskiq import Flow, InMemoryBroker, TaskiqRouter

router = TaskiqRouter()

default_email_flow = Flow.queue("emails.default")
priority_email_flow = Flow.queue("emails.priority")
bulk_email_flow = Flow.queue("emails.bulk")

default_broker = InMemoryBroker(
    router=router,
    broker_name="default",
    default_flow=default_email_flow,
    await_inplace=True,
)
priority_broker = InMemoryBroker(
    router=router,
    broker_name="priority",
    default_flow=priority_email_flow,
    await_inplace=True,
)


@default_broker.task(task_name="examples.send_email", domain="notifications")
async def send_email(user_id: int, template: str) -> str:
    """Pretend to render and send an email."""
    return f"{template} email sent to user {user_id}"


router.route_task(
    send_email.task_name,
    broker="priority",
    flow=priority_email_flow,
)


async def _main() -> None:
    await default_broker.startup()
    await priority_broker.startup()
    try:
        direct_result = await send_email(7, "welcome")

        routed_task = await send_email.kiq(7, "welcome")
        routed_result = await routed_task.wait_result(timeout=2)

        bulk_task = await send_email.kicker().with_route(
            "default",
            bulk_email_flow,
        ).kiq(8, "digest")
        bulk_result = await bulk_task.wait_result(timeout=2)

        print(f"Direct call: {direct_result}")
        print(f"Default route: {router.resolve_route(send_email.task_name)}")
        print(f"Routed call: {routed_result.return_value}")
        print(f"Route override: {bulk_result.return_value}")
    finally:
        await priority_broker.shutdown()
        await default_broker.shutdown()


if __name__ == "__main__":
    asyncio.run(_main())
