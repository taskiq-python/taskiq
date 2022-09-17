import asyncio
from typing import Optional

from redis.asyncio import ConnectionPool, Redis  # type: ignore
from taskiq_aio_pika import AioPikaBroker
from taskiq_redis import RedisAsyncResultBackend

from taskiq import Context, TaskiqEvents, TaskiqState
from taskiq.context import default_context

# To run this example, please install:
# * taskiq
# * taskiq-redis
# * taskiq-aio-pika

broker = AioPikaBroker(
    "amqp://localhost",
    result_backend=RedisAsyncResultBackend(
        "redis://localhost/0",
    ),
)


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def startup(state: TaskiqState) -> None:
    # Here we store connection pool on startup for later use.
    state.redis = ConnectionPool.from_url("redis://localhost/1")


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def shutdown(state: TaskiqState) -> None:
    # Here we close our pool on shutdown event.
    await state.redis.disconnect()


@broker.task
async def get_val(key: str, context: Context = default_context) -> Optional[str]:
    # Now we can use our pool.
    redis = Redis(connection_pool=context.state.redis, decode_responses=True)
    return await redis.get(key)


@broker.task
async def set_val(key: str, value: str, context: Context = default_context) -> None:
    # Now we can use our pool to set value.
    await Redis(connection_pool=context.state.redis).set(key, value)


async def main() -> None:
    await broker.startup()

    set_task = await set_val.kiq("key", "value")
    set_result = await set_task.wait_result(with_logs=True)
    if set_result.is_err:
        print(set_result.log)
        raise ValueError("Cannot set value in redis. See logs.")

    get_task = await get_val.kiq("key")
    get_res = await get_task.wait_result()
    print(f"Got redis value: {get_res.return_value}")

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
