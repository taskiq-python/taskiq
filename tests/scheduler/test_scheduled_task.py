import pytest

from taskiq.scheduler.scheduled_task import ScheduledTask


def test_scheduled_task_paramters() -> None:
    with pytest.raises(ValueError):
        ScheduledTask(
            task_name="a",
            labels={},
            args=[],
            kwargs={},
            schedule_id="b",
        )
