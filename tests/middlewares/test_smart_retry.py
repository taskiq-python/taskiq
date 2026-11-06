import datetime
import uuid
from unittest.mock import AsyncMock

import pytest

from taskiq import InMemoryBroker, ScheduleSource, ScheduledTask, SmartRetryMiddleware
from taskiq.formatters.json_formatter import JSONFormatter
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult


@pytest.fixture
def broker() -> AsyncMock:
    """Create a mock broker for testing."""
    mocked_broker = AsyncMock()
    mocked_broker.id_generator = lambda: uuid.uuid4().hex
    mocked_broker.formatter = JSONFormatter()
    mocked_broker.find_task = lambda task_name: None
    return mocked_broker


class MockScheduleSource(ScheduleSource):
    """Mock schedule source recording last scheduled task."""

    def __init__(self) -> None:
        self.last: ScheduledTask | None = None

    async def get_schedules(self) -> list[ScheduledTask]:
        return []

    async def add_schedule(self, schedule: ScheduledTask) -> None:
        self.last = schedule


async def test_smart_retry_delay_exponent_without_schedule_source() -> None:
    """Ensure retry delay does not compound without schedule_source."""
    mw = SmartRetryMiddleware(
        default_retry_label=True,
        default_retry_count=6,
        default_delay=5.0,
        use_delay_exponent=True,
        max_delay_exponent=60.0,
        schedule_source=None,
    )
    broker = InMemoryBroker().with_middlewares(mw)
    last_message: TaskiqMessage | None = None

    async def mock_kick(message: TaskiqMessage) -> None:
        nonlocal last_message
        last_message = message

    broker.kick = mock_kick  # type: ignore[assignment]

    labels: dict[str, str | float] = {}
    delays: list[float] = []

    for _ in range(5):
        msg = TaskiqMessage(
            task_id="test-task",
            task_name="test-task",
            labels=labels,
            args=[],
            kwargs={},
        )
        result: TaskiqResult[None] = TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.0,
        )
        await mw.on_error(msg, result, ValueError("retry me"))
        assert last_message is not None
        labels = dict(last_message.labels)
        delays.append(round(float(labels["delay"]), 1))

    # Delay should scale linearly (5, 10, 15, 20, 25), not compound (5, 10, 30, ...)
    assert delays == [5.0, 10.0, 15.0, 20.0, 25.0]


async def test_smart_retry_consistency_with_and_without_schedule_source() -> None:
    """Verify delays match between broker path and schedule_source path."""
    source = MockScheduleSource()
    mw_broker = SmartRetryMiddleware(
        default_retry_label=True,
        default_retry_count=6,
        default_delay=5.0,
        use_delay_exponent=True,
        max_delay_exponent=60.0,
        schedule_source=None,
    )
    mw_source = SmartRetryMiddleware(
        default_retry_label=True,
        default_retry_count=6,
        default_delay=5.0,
        use_delay_exponent=True,
        max_delay_exponent=60.0,
        schedule_source=source,
    )

    broker_for_direct = InMemoryBroker().with_middlewares(mw_broker)
    last_direct_message: TaskiqMessage | None = None

    async def mock_kick(message: TaskiqMessage) -> None:
        nonlocal last_direct_message
        last_direct_message = message

    broker_for_direct.kick = mock_kick  # type: ignore[assignment]
    InMemoryBroker().with_middlewares(mw_source)

    direct_labels: dict[str, str | float] = {}
    source_labels: dict[str, str | float] = {}
    direct_delays: list[float] = []
    source_delays: list[float] = []

    for _ in range(4):
        now = datetime.datetime.now(datetime.timezone.utc)
        # Broker path
        msg_direct = TaskiqMessage(
            task_id="t1",
            task_name="t1",
            labels=direct_labels,
            args=[],
            kwargs={},
        )
        res_direct: TaskiqResult[None] = TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.0,
        )
        await mw_broker.on_error(msg_direct, res_direct, ValueError())
        assert last_direct_message is not None
        direct_labels = dict(last_direct_message.labels)
        direct_delays.append(round(float(direct_labels["delay"]), 1))

        # Schedule source path
        msg_source = TaskiqMessage(
            task_id="t2",
            task_name="t2",
            labels=source_labels,
            args=[],
            kwargs={},
        )
        res_source: TaskiqResult[None] = TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.0,
        )
        await mw_source.on_error(msg_source, res_source, ValueError())
        assert source.last is not None and source.last.time is not None
        source_labels = dict(source.last.labels)
        source_delays.append(
            round((source.last.time - now).total_seconds(), 1),
        )

    assert direct_delays == source_delays == [5.0, 10.0, 15.0, 20.0]


async def test_smart_retry_jitter_does_not_accumulate() -> None:
    """Verify jitter is not compounded into the base delay on each retry."""
    mw = SmartRetryMiddleware(
        default_retry_label=True,
        default_retry_count=5,
        default_delay=10.0,
        use_jitter=True,
        use_delay_exponent=False,
        schedule_source=None,
    )
    broker = InMemoryBroker().with_middlewares(mw)
    last_message: TaskiqMessage | None = None

    async def mock_kick(message: TaskiqMessage) -> None:
        nonlocal last_message
        last_message = message

    broker.kick = mock_kick  # type: ignore[assignment]

    labels: dict[str, str | float] = {}
    for _ in range(4):
        msg = TaskiqMessage(
            task_id="t",
            task_name="t",
            labels=labels,
            args=[],
            kwargs={},
        )
        result: TaskiqResult[None] = TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.0,
        )
        await mw.on_error(msg, result, ValueError())
        assert last_message is not None
        labels = dict(last_message.labels)
        delay = float(labels["delay"])
        # Delay should be base 10.0 + random(0, 1), so always strictly < 11.0
        assert 10.0 <= delay <= 11.0


async def test_smart_retry_custom_delay_label() -> None:
    """Verify custom delay label on message is preserved as initial delay."""
    mw = SmartRetryMiddleware(
        default_retry_label=True,
        default_retry_count=5,
        default_delay=5.0,
        use_delay_exponent=True,
        max_delay_exponent=100.0,
    )
    broker = InMemoryBroker().with_middlewares(mw)
    last_message: TaskiqMessage | None = None

    async def mock_kick(message: TaskiqMessage) -> None:
        nonlocal last_message
        last_message = message

    broker.kick = mock_kick  # type: ignore[assignment]

    labels: dict[str, str | float] = {"delay": 12.0}
    delays: list[float] = []

    for _ in range(3):
        msg = TaskiqMessage(
            task_id="t",
            task_name="t",
            labels=labels,
            args=[],
            kwargs={},
        )
        result: TaskiqResult[None] = TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.0,
        )
        await mw.on_error(msg, result, ValueError())
        assert last_message is not None
        labels = dict(last_message.labels)
        delays.append(round(float(labels["delay"]), 1))

    assert delays == [12.0, 24.0, 36.0]
