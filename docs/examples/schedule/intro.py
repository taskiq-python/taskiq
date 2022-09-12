from taskiq_aio_pika import AioPikaBroker

from taskiq.schedule_sources import LabelScheduleSource
from taskiq.scheduler import TaskiqScheduler

broker = AioPikaBroker("amqp://guest:guest@localhost:5672/")

scheduler = TaskiqScheduler(
    broker=broker,
    sources=[LabelScheduleSource(broker)],
)


@broker.task(schedule=[{"cron": "*/5 * * * *", "args": [1]}])
async def heavy_task(value: int) -> int:
    return value + 1
