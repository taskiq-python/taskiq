"""Regression tests for #605.

A ``timedelta`` offset is serialized by pydantic to an ISO-8601 duration
string (e.g. ``"PT4H"``). On reload the ``str | timedelta`` union kept it as
``str``, which then broke timezone handling in the scheduler
(``ZoneInfo("PT4H")``). The offset must round-trip back to ``timedelta``
while genuine timezone names stay untouched.
"""

from datetime import timedelta

from taskiq.compat import model_dump, model_validate
from taskiq.scheduler.scheduled_task import CronSpec, ScheduledTask


def test_cron_spec_timedelta_offset_roundtrip() -> None:
    offset = timedelta(hours=4)
    restored = model_validate(CronSpec, model_dump(CronSpec(offset=offset)))
    assert restored.offset == offset
    assert isinstance(restored.offset, timedelta)


def test_cron_spec_timezone_name_offset_preserved() -> None:
    restored = model_validate(CronSpec, model_dump(CronSpec(offset="US/Eastern")))
    assert restored.offset == "US/Eastern"


def test_scheduled_task_timedelta_cron_offset_roundtrip() -> None:
    offset = timedelta(hours=2)
    task = ScheduledTask(
        task_name="a",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
        cron_offset=offset,
    )
    restored = model_validate(ScheduledTask, model_dump(task))
    assert restored.cron_offset == offset
    assert isinstance(restored.cron_offset, timedelta)


def test_scheduled_task_timezone_name_cron_offset_preserved() -> None:
    task = ScheduledTask(
        task_name="a",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
        cron_offset="US/Eastern",
    )
    restored = model_validate(ScheduledTask, model_dump(task))
    assert restored.cron_offset == "US/Eastern"
