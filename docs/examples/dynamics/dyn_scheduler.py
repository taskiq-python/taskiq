import asyncio
import datetime

from taskiq_redis import ListQueueBroker

from taskiq import TaskiqScheduler
from taskiq.api import run_receiver_task, run_scheduler_task
from taskiq.schedule_sources import LabelScheduleSource


async def main() -> None:
    # Here we define a broker.
    dyn_broker = ListQueueBroker("redis://localhost")
    dyn_scheduler = TaskiqScheduler(dyn_broker, [LabelScheduleSource(dyn_broker)])

    await dyn_broker.startup()

    # Now we register lambda as a task.
    dyn_task = dyn_broker.register_task(
        lambda x: print("A", x),
        task_name="dyn_task",
        # We add a schedule when to run task.
        schedule=[
            {
                # Here we also can specify cron instead of time.
                "time": datetime.datetime.now(datetime.UTC)
                + datetime.timedelta(seconds=2),
                "args": [22],
            },
        ],
    )

    # We create scheduler after the task declaration,
    # so we don't have to wait a minute before it gets to the task.
    # However, defining a scheduler before the task declaration is also possible.
    # But we have to wait till it gets to task execution for the second time.
    worker_task = asyncio.create_task(run_receiver_task(dyn_broker))
    scheduler_task = asyncio.create_task(run_scheduler_task(dyn_scheduler))

    # We still able to send the task.
    await dyn_task.kiq(x=1)

    await asyncio.sleep(10)

    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        print("Worker successfully exited.")

    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        print("Scheduler successfully exited.")

    await dyn_broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
