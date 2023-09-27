import asyncio

from taskiq_redis import ListQueueBroker

from taskiq.api import run_receiver_task


async def main() -> None:
    # Here we define a broker.
    dyn_broker = ListQueueBroker("redis://localhost")
    await dyn_broker.startup()
    worker_task = asyncio.create_task(run_receiver_task(dyn_broker))

    # Now we register lambda as a task.
    dyn_task = dyn_broker.register_task(
        lambda x: print("A", x),
        task_name="dyn_task",
    )

    # Now we can send it.
    await dyn_task.kiq(x=1)

    await asyncio.sleep(2)

    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        print("Worker successfully exited.")

    await dyn_broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
