import os

from taskiq import AsyncBroker, InMemoryBroker, ZeroMQBroker

env = os.environ.get("ENVIRONMENT")

broker: AsyncBroker = ZeroMQBroker()

if env and env == "pytest":
    broker = InMemoryBroker()
