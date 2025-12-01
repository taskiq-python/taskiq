from datetime import datetime

from taskiq import InMemoryBroker, ScheduleSource
from taskiq.cli.scheduler.run import get_all_schedules
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler


class DummySource(ScheduleSource):
    def __init__(self, schedules: Exception | list[ScheduledTask]) -> None:
        self.schedules = schedules

    async def get_schedules(self) -> list[ScheduledTask]:
        """Return test schedules, or raise an exception."""
        if isinstance(self.schedules, Exception):
            raise self.schedules
        return self.schedules


async def test_get_schedules_success() -> None:
    """Tests that schedules are returned correctly."""
    schedules1 = [
        ScheduledTask(
            task_name="a",
            labels={},
            args=[],
            kwargs={},
            time=datetime.now(),
        ),
        ScheduledTask(
            task_name="b",
            labels={},
            args=[],
            kwargs={},
            time=datetime.now(),
        ),
    ]
    schedules2 = [
        ScheduledTask(
            task_name="c",
            labels={},
            args=[],
            kwargs={},
            time=datetime.now(),
        ),
    ]
    sources: list[ScheduleSource] = [
        DummySource(schedules1),
        DummySource(schedules2),
    ]

    schedules = await get_all_schedules(
        TaskiqScheduler(InMemoryBroker(), sources),
    )
    assert schedules == [
        (sources[0], schedules1),
        (sources[1], schedules2),
    ]


async def test_get_schedules_error() -> None:
    """Tests that if source returned an error, empty list will be returned."""
    source1 = DummySource(
        [
            ScheduledTask(
                task_name="a",
                labels={},
                args=[],
                kwargs={},
                time=datetime.now(),
            ),
        ],
    )
    source2 = DummySource(Exception("test"))

    schedules = await get_all_schedules(
        TaskiqScheduler(InMemoryBroker(), [source1, source2]),
    )
    assert schedules == [
        (source1, source1.schedules),
        (source2, []),
    ]
