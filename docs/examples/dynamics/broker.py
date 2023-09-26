import asyncio

from taskiq_redis import ListQueueBroker


async def main() -> None:
    # Here we define a broker.
    dyn_broker = ListQueueBroker("redis://localhost")
    await dyn_broker.startup()

    # Now we register lambda as a task.
    dyn_task = dyn_broker.register_task(
        lambda x: print("A", x),
        task_name="dyn_task",
    )

    # now we can send it.
    await dyn_task.kiq(x=1)

    await dyn_broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
