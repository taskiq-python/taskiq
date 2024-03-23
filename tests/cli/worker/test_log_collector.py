import asyncio
import logging

import pytest

from taskiq.cli.worker.log_collector import TaskiqLogHandler


@pytest.mark.anyio
async def test_log_collector_success() -> None:
    """Tests that logs are collected correctly."""
    handler = TaskiqLogHandler()
    logger = logging.getLogger("taskiq.tasklogger")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    handler.associate("someid")
    logger.info("Thing 1")
    logger.info("Thing 2")
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return
    else:
        if task:
            task_name = task.get_name()
        else:
            raise RuntimeError
    assert [record.message for record in handler.stream[task_name]] == [
        "Thing 1",
        "Thing 2",
    ]
