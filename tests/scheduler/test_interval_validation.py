from datetime import datetime, timedelta
from typing import Union

import pytest

from taskiq.scheduler.scheduled_task import ScheduledTask


def test_valid_interval_tasks() -> None:
    """Test that valid interval tasks are created successfully."""
    task1 = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        interval=30,
    )
    assert task1.interval == 30

    task2 = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        interval=timedelta(minutes=5),
    )
    assert task2.interval == timedelta(minutes=5)

    task3 = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        interval=1,
    )
    assert task3.interval == 1


@pytest.mark.parametrize(
    "interval",
    [
        0,
        -5,
        timedelta(seconds=0),
        timedelta(seconds=0.5),
        timedelta(microseconds=500000),
        timedelta(seconds=-1),
    ],
)
def test_invalid_interval_tasks(interval: Union[int, timedelta]) -> None:
    """Test that invalid interval tasks raise ValueError."""
    with pytest.raises(ValueError):
        ScheduledTask(
            task_name="test_task",
            labels={},
            args=[],
            kwargs={},
            interval=interval,
        )


def test_interval_validation_with_other_schedule_types() -> None:
    """Test that interval validation works with other schedule types."""
    task1 = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
        interval=30,
    )
    assert task1.interval == 30

    task2 = ScheduledTask(
        task_name="test_task",
        labels={},
        args=[],
        kwargs={},
        time=datetime.now(),
        interval=timedelta(minutes=5),
    )
    assert task2.interval == timedelta(minutes=5)

    with pytest.raises(ValueError, match="Interval must be at least 1 second"):
        ScheduledTask(
            task_name="test_task",
            labels={},
            args=[],
            kwargs={},
            cron="* * * * *",
            interval=0,
        )
