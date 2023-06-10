from typing import Generator

import pytest

from taskiq.abc.broker import AsyncBroker


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """
    Anyio backend.

    Backend for anyio pytest plugin.
    :return: backend name.
    """
    return "asyncio"


@pytest.fixture(autouse=True)
def reset_broker() -> Generator[None, None, None]:
    """
    Restore async broker.

    This fixtures sets some global
    broker variables to default state.
    """
    yield
    AsyncBroker.available_tasks = {}
    AsyncBroker.is_worker_process = False
    AsyncBroker.is_scheduler_process = False
