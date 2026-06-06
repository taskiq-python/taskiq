"""Route one task through several brokers with a shared router."""

import asyncio

from taskiq import Flow, InMemoryBroker, TaskiqRoute, TaskiqRouter

router = TaskiqRouter()

default_email_flow = Flow("emails.default")
priority_email_flow = Flow("emails.priority")
bulk_email_flow = Flow("emails.bulk")

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


priority_route = router.route_task(
    send_email,
    broker=priority_broker,
    flow=priority_email_flow,
)
priority_subscription = router.subscribe(
    priority_broker,
    priority_email_flow,
    send_email,
)


def _format_route(task_name: str, route: TaskiqRoute) -> str:
    """Return a readable route diagnostic for the example output."""
    flow_name = route.flow.name if route.flow is not None else "<default>"
    return f"{task_name} -> broker={route.broker_name}, flow={flow_name}"


def _format_listen_plan() -> str:
    """Return flows that the priority broker should subscribe to."""
    flow_names = ", ".join(flow.name for flow in priority_broker.get_subscribed_flows())
    return f"priority listens to: {flow_names}"


async def _main() -> None:
    await default_broker.startup()
    await priority_broker.startup()
    try:
        direct_result = await send_email(7, "welcome")

        declared_route = router.resolve_route(send_email)
        assert declared_route == priority_route

        routed_task = (
            await send_email.kicker()
            .with_route(declared_route)
            .kiq(
                7,
                "welcome",
            )
        )
        routed_result = await routed_task.wait_result(timeout=2)

        bulk_route = router.resolve_route(
            send_email,
            broker=default_broker,
            flow=bulk_email_flow,
        )
        bulk_task = await send_email.kicker().with_route(bulk_route).kiq(8, "digest")
        bulk_result = await bulk_task.wait_result(timeout=2)

        print(f"Direct call: {direct_result}")
        print(f"Router rule: {_format_route(send_email.task_name, priority_route)}")
        print(f"Subscription tasks: {sorted(priority_subscription.task_names)}")
        print(_format_listen_plan())
        print(f"Resolved route: {_format_route(send_email.task_name, declared_route)}")
        print(f"Routed call: {routed_result.return_value}")
        print(f"Override route: {_format_route(send_email.task_name, bulk_route)}")
        print(f"Route override: {bulk_result.return_value}")
    finally:
        await priority_broker.shutdown()
        await default_broker.shutdown()


if __name__ == "__main__":
    asyncio.run(_main())
