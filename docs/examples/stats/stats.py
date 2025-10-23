from typing import Any
import asyncio
import os
import random

from redis.asyncio import Redis
from taskiq import AsyncBroker, AsyncTaskiqTask, Context, TaskiqDepends, TaskiqResult
from taskiq.api import run_receiver_task
from taskiq.exceptions import ResultGetError
from taskiq.metrics import Stat, Stats
from taskiq_redis import ListQueueBroker, PubSubBroker, RedisAsyncResultBackend
from taskiq_redis.redis_broker import BaseRedisBroker
from taskiq.middlewares.stat_middleware import StatMiddleware

random.seed()

redis_result_url = "redis://localhost:6379/0"
redis_stat_url = "redis://localhost:6379/1"

task_async_result: RedisAsyncResultBackend[Any] = RedisAsyncResultBackend(
    redis_url=redis_result_url,
)

broker = ListQueueBroker(url=redis_result_url).with_result_backend(
    task_async_result,
)

stat_async_result: RedisAsyncResultBackend[Any] = RedisAsyncResultBackend(
    redis_url=redis_stat_url,
)

stat_broker = PubSubBroker(url=redis_stat_url).with_result_backend(
    stat_async_result,
)

stat_middleware = StatMiddleware(stat_broker=stat_broker)
broker.add_middlewares(stat_middleware)


async def get_task_result(
    broker: AsyncBroker,
    task_id: str,
) -> TaskiqResult[Any] | None:
    """Get task result from redis by task_id."""
    task = AsyncTaskiqTask(task_id=task_id, result_backend=broker.result_backend)
    try:
        if task_result := await task.get_result():
            return task_result
    except ResultGetError:
        pass
    return None


async def get_keys(
    broker: AsyncBroker,
    prefix: str,
    max_count: int = 50,
) -> list[str]:
    """Get redis keys via scan by prefix."""
    keys = []
    if isinstance(broker, BaseRedisBroker) and isinstance(
        broker.result_backend,
        RedisAsyncResultBackend,
    ):
        async with Redis(connection_pool=broker.result_backend.redis_pool) as redis:
            async for key in redis.scan_iter(f"{prefix}:*"):
                keys.append(key.decode() if isinstance(key, bytes) else key)
                if len(keys) == max_count:
                    break
    return keys


@stat_broker.task()
async def get_stats(context: Context = TaskiqDepends()) -> Stat:
    """
    Task to get stats from StatMiddleware instance of each worker process.

    As soon as we use pub-sub broker inside StatMiddleware, and it starts inside each
    worker process, the result value will be overwritten inside result_backend.
    So we need to change task_id of the task to be able to gather many results
    from different workers by adding process pid to the initial task_id.

    To get all results one must scan result_backend with pattern:
    task_id:*
    and aggregate all results together.
    """
    context.message.task_id = f"{context.message.task_id}:{os.getpid()}"
    return stat_middleware.get_stats()


@broker.task()
async def get_all_stats(timeout: float = 0.2) -> Stats:
    """Gathers results of get_stats task from all running workers."""
    results = {}
    if task := await get_stats.kiq():
        task_id = task.task_id
        await asyncio.sleep(timeout)
        if keys := await get_keys(
            broker=stat_broker,
            prefix=task_id,
        ):
            for key in keys:
                try:
                    _, worker_pid = key.split(":")
                except ValueError:
                    continue
                if result := await get_task_result(broker=stat_broker, task_id=key):
                    results[int(worker_pid)] = result.return_value
    return Stats(workers=results)


@broker.task()
async def demo_task(timeout: float = 0.1) -> bool:
    print(f"demo_task(timeout={timeout})")
    await asyncio.sleep(timeout)
    return random.choice([True, False])


async def main() -> None:
    # Emulate we run taskiq worker processes with several workers.
    broker.is_worker_process = True
    # Await broker startup with stat_middleware startup that starts pub-sub worker
    await broker.startup()
    # Start random number of workers
    worker_tasks = []
    for _ in range(random.randint(2, 5)):
        worker_task = asyncio.create_task(run_receiver_task(broker))
        worker_tasks.append(worker_task)

    # Start random number of tasks with random execution time
    for _ in range(random.randint(2, 10)):
        await demo_task.kiq(timeout=random.random())

    # Wait a little
    await asyncio.sleep(0.5)

    # Start task to gather stats from all workers
    get_stats_task = await get_all_stats.kiq()
    stats_result = await get_stats_task.wait_result()
    if stats_result:
        print("Stats of all workers:\n\t", stats_result.return_value)

    # Stop workers.
    for worker_task in worker_tasks:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            print("Worker successfully exited.")

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
