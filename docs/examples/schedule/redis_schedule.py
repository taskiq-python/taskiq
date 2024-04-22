from taskiq_redis import ListQueueBroker, RedisScheduleSource

from taskiq import TaskiqScheduler

# Here's the broker that is going to execute tasks
broker = ListQueueBroker("redis://localhost:6379/0")

# Here's the source that is used to store scheduled tasks
redis_source = RedisScheduleSource("redis://localhost:6379/0")

# And here's the scheduler that is used to query scheduled sources
scheduler = TaskiqScheduler(broker, sources=[redis_source])


@broker.task
async def my_task(arg1: int, arg2: str) -> None:
    """Example task."""
    print("Hello from my_task!", arg1, arg2)  # noqa: T201
