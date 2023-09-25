import asyncio
from typing import Generator

import pytest
from pytest_mock import MockerFixture

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
    AsyncBroker.global_task_registry = {}
    AsyncBroker.is_worker_process = False
    AsyncBroker.is_scheduler_process = False


@pytest.fixture
def mock_sleep(mocker: MockerFixture) -> None:
    async def _fast_sleep(delay: float) -> None:
        await asyncio_sleep(delay / 10000)

    asyncio_sleep = asyncio.sleep
    mocker.patch("asyncio.sleep", _fast_sleep)
