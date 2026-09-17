from datetime import timedelta
from typing import Any

import pytest

from taskiq.scheduler.scheduled_task import CronSpec, ScheduledTask


@pytest.mark.parametrize(
    ("offset", "offset_type"),
    [
        (timedelta(hours=4), timedelta),
        ("US/Eastern", str),
    ],
)
def test_cron_spec_offset_roundtrip(offset: Any, offset_type: type) -> None:
    restored = CronSpec.model_validate(CronSpec(offset=offset).model_dump(mode="json"))
    assert restored.offset == offset
    assert type(restored.offset) is offset_type


@pytest.mark.parametrize(
    ("offset", "offset_type"),
    [
        (timedelta(hours=2), timedelta),
        ("US/Eastern", str),
    ],
)
def test_scheduled_task_cron_offset_roundtrip(offset: Any, offset_type: type) -> None:
    task = ScheduledTask(
        task_name="a",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
        cron_offset=offset,
    )
    restored = ScheduledTask.model_validate(task.model_dump(mode="json"))
    assert restored.cron_offset == offset
    assert type(restored.cron_offset) is offset_type


def test_scheduled_task_parameters() -> None:
    with pytest.raises(ValueError):
        ScheduledTask(
            task_name="a",
            labels={},
            args=[],
            kwargs={},
            schedule_id="b",
        )


def test_scheduled_task_interval() -> None:
    with pytest.raises(ValueError):
        ScheduledTask(
            task_name="a",
            labels={},
            args=[],
            kwargs={},
            schedule_id="b",
            interval=-1,
        )
