"""Abstract classes for taskiq."""

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend

__all__ = [
    "AsyncBroker",
    "AsyncResultBackend",
]
