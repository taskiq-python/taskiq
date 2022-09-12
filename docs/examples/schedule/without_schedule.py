from taskiq_aio_pika import AioPikaBroker

broker = AioPikaBroker("amqp://guest:guest@localhost:5672/")


@broker.task
async def heavy_task(value: int) -> int:
    return value + 1
