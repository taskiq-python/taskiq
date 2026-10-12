from datetime import datetime, timezone

import pycron
import pytest
from pydantic import ValidationError

from taskiq.scheduler.scheduled_task import Cron, CronError, CronSpec, ScheduledTask


@pytest.mark.parametrize(
    "expr",
    [
        "* * * * *",
        "0 0 1 1 *",
        "*/5 1-3 1,15 JAN-MAR MON-FRI",
        "0-30/10 * * * sun",
        "0 0 * * 7",
        "0 0 * * 5-7",
    ],
)
def test_valid(expr: str) -> None:
    assert str(Cron.parse(expr)) == expr


@pytest.mark.parametrize(
    "expr",
    [
        "invalid cron",
        "* * * *",
        "60 * * * *",
        "* 24 * * *",
        "* * 0 * *",
        "* * * 13 *",
        "* * * * 8",
        "*/0 * * * *",
        "5/10 * * * *",
        "5-1 * * * *",
        "1,,2 * * * *",
        "@daily",
    ],
)
def test_invalid(expr: str) -> None:
    with pytest.raises(CronError):
        Cron.parse(expr)


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        ("* * * * MON-FRI", "* * * * 1,2,3,4,5"),
        ("* * * * 7", "* * * * 0"),
        ("* * * * 5-7", "* * * * 5,6,0"),
        ("* * * jan-mar *", "* * * 1-3 *"),
        ("*/5 0-20/10 * * *", "*/5 0-20/10 * * *"),
    ],
)
def test_to_pycron(expr: str, expected: str) -> None:
    assert Cron.parse(expr).to_pycron() == expected


def test_pycron_matches_sunday() -> None:
    sunday = datetime(2023, 1, 1, tzinfo=timezone.utc)
    for expr in ("* * * * SUN", "* * * * 7", "* * * * 5-7"):
        assert pycron.is_now(Cron.parse(expr).to_pycron(), sunday)


def test_scheduled_task_cron() -> None:
    task = ScheduledTask(
        task_name="a",
        labels={},
        args=[],
        kwargs={},
        cron="*/5 * * * *",
    )
    assert isinstance(task.cron, Cron)
    dumped = task.model_dump(mode="json")
    assert dumped["cron"] == "*/5 * * * *"
    assert ScheduledTask.model_validate(dumped).cron == task.cron


def test_scheduled_task_invalid_cron() -> None:
    with pytest.raises(ValidationError):
        ScheduledTask(task_name="a", labels={}, args=[], kwargs={}, cron="bad")


def test_cron_spec() -> None:
    assert str(CronSpec(minutes=5).to_cron()) == "5 * * * *"
    with pytest.raises(ValidationError):
        CronSpec(minutes=99)
