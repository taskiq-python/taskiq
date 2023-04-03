"""Package for message receiver with utils."""
from taskiq.receiver.async_task_runner import async_listen_messages
from taskiq.receiver.receiver import Receiver

__all__ = [
    "Receiver",
    "async_listen_messages",
]
