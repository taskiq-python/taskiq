import asyncio
import json
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from taskiq import TaskiqScheduler
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler_plugins import TaskiqAdminSchedulerPlugin
from tests.utils import AsyncQueueBroker

AdminState = dict[str, list[Any]]


class EditableScheduleSource(ScheduleSource):
    def __init__(self, schedules: list[ScheduledTask] | None = None) -> None:
        self.schedules = {
            schedule.schedule_id: schedule for schedule in schedules or []
        }

    async def get_schedules(self) -> list["ScheduledTask"]:
        """Return schedules list."""
        return list(self.schedules.values())

    async def add_schedule(self, schedule: "ScheduledTask") -> None:
        """Add a new schedule."""
        self.schedules[schedule.schedule_id] = schedule

    async def delete_schedule(self, schedule_id: str) -> None:
        """Delete a schedule by id."""
        self.schedules.pop(schedule_id, None)

    async def post_send(self, task: "ScheduledTask") -> None:
        """Delete one-off schedules after they fire."""
        if task.time is not None:
            self.schedules.pop(task.schedule_id, None)


class ReadOnlyScheduleSource(ScheduleSource):
    async def get_schedules(self) -> list["ScheduledTask"]:
        """Return schedules list."""
        return []


@pytest.fixture
async def admin_server() -> AsyncGenerator[tuple[TestServer, AdminState], None]:
    state: AdminState = {
        "snapshots": [],
        "tasks_snapshots": [],
        "acks": [],
        "commands": [],
    }

    async def handle_snapshot(request: web.Request) -> web.Response:
        state["snapshots"].append(await request.json())
        return web.json_response({"success": True})

    async def handle_tasks_snapshot(request: web.Request) -> web.Response:
        state["tasks_snapshots"].append(await request.json())
        return web.json_response({"success": True})

    async def handle_poll(request: web.Request) -> web.Response:
        await request.json()
        commands, state["commands"] = state["commands"], []
        return web.json_response({"commands": commands})

    async def handle_ack(request: web.Request) -> web.Response:
        state["acks"].append(await request.json())
        return web.json_response({"success": True})

    app = web.Application()
    app.router.add_post("/api/schedules/snapshot", handle_snapshot)
    app.router.add_post("/api/schedules/tasks-snapshot", handle_tasks_snapshot)
    app.router.add_post("/api/schedules/commands/poll", handle_poll)
    app.router.add_post("/api/schedules/commands/ack", handle_ack)

    server = TestServer(app)
    await server.start_server()
    yield server, state
    await server.close()


def make_scheduler(
    server: TestServer,
    broker: AsyncQueueBroker,
    sources: list[ScheduleSource],
) -> tuple[TaskiqAdminSchedulerPlugin, TaskiqScheduler]:
    plugin = TaskiqAdminSchedulerPlugin(
        str(server.make_url("/")),
        "supersecret",
        poll_interval=0.05,
    )
    scheduler = TaskiqScheduler(broker, sources=sources, plugins=[plugin])
    return plugin, scheduler


def get_schedule() -> ScheduledTask:
    return ScheduledTask(
        task_name="ping:pong",
        labels={},
        args=[],
        kwargs={},
        cron="* * * * *",
    )


async def test_startup_reports_registered_tasks(
    admin_server: tuple[TestServer, AdminState],
) -> None:
    server, state = admin_server
    broker = AsyncQueueBroker()

    @broker.task(task_name="ping:pong")
    def _() -> None: ...

    _, scheduler = make_scheduler(server, broker, [EditableScheduleSource()])
    await scheduler.startup()
    await scheduler.shutdown()

    assert state["tasks_snapshots"]
    names = [task["name"] for task in state["tasks_snapshots"][0]["tasks"]]
    assert "ping:pong" in names


async def test_snapshot_pushed_on_schedules_updated(
    admin_server: tuple[TestServer, AdminState],
) -> None:
    server, state = admin_server
    schedule = get_schedule()
    source = EditableScheduleSource([schedule])
    plugin, scheduler = make_scheduler(server, AsyncQueueBroker(), [source])
    plugin._resolve_names()

    await scheduler.on_schedules_updated(source, [schedule])

    assert len(state["snapshots"]) == 1
    snapshot = state["snapshots"][0]
    assert snapshot["sourceName"] == "EditableScheduleSource"
    assert snapshot["editable"] is True
    item = snapshot["schedules"][0]
    assert item["scheduleId"] == schedule.schedule_id
    assert item["taskName"] == "ping:pong"
    assert item["cron"] == "* * * * *"
    assert item["opaque"] is False


async def test_opaque_fallback_for_unserializable_args(
    admin_server: tuple[TestServer, AdminState],
) -> None:
    server, state = admin_server
    schedule = get_schedule()
    schedule.args = [object()]
    source = EditableScheduleSource([schedule])
    plugin, scheduler = make_scheduler(server, AsyncQueueBroker(), [source])
    plugin._resolve_names()

    await scheduler.on_schedules_updated(source, [schedule])

    item = state["snapshots"][0]["schedules"][0]
    assert item["opaque"] is True
    assert "object object" in item["args"][0]


async def test_delete_command_applied(
    admin_server: tuple[TestServer, AdminState],
) -> None:
    server, state = admin_server
    schedule = get_schedule()
    source = EditableScheduleSource([schedule])
    plugin, _ = make_scheduler(server, AsyncQueueBroker(), [source])
    plugin._resolve_names()
    state["commands"] = [
        {
            "id": "cmd-1",
            "type": "delete",
            "payload": {"schedule_id": schedule.schedule_id},
        },
    ]

    await plugin._poll_once()

    assert schedule.schedule_id not in source.schedules
    assert state["acks"][0]["results"] == [
        {"id": "cmd-1", "status": "applied", "error": None},
    ]
    # The plugin pushes a fresh snapshot right after applying commands.
    assert state["snapshots"][-1]["schedules"] == []


async def test_add_command_applied(
    admin_server: tuple[TestServer, AdminState],
) -> None:
    server, state = admin_server
    source = EditableScheduleSource()
    plugin, _ = make_scheduler(server, AsyncQueueBroker(), [source])
    plugin._resolve_names()
    state["commands"] = [
        {
            "id": "cmd-2",
            "type": "add",
            "payload": {
                "schedule_id": "new-schedule",
                "task_name": "ping:pong",
                "labels": {},
                "args": [],
                "kwargs": {},
                "cron": "0 3 * * *",
            },
        },
    ]

    await plugin._poll_once()

    assert "new-schedule" in source.schedules
    assert source.schedules["new-schedule"].cron == "0 3 * * *"
    assert state["acks"][0]["results"][0]["status"] == "applied"


async def test_trigger_command_kicks_with_task_id(
    admin_server: tuple[TestServer, AdminState],
) -> None:
    server, state = admin_server
    broker = AsyncQueueBroker()
    plugin, _ = make_scheduler(server, broker, [EditableScheduleSource()])
    plugin._resolve_names()
    state["commands"] = [
        {
            "id": "cmd-3",
            "type": "trigger",
            "payload": {
                "task_name": "ping:pong",
                "labels": {},
                "args": [1],
                "kwargs": {"key": "value"},
                "task_id": "custom-task-id",
            },
        },
    ]

    await plugin._poll_once()

    message = json.loads(await asyncio.wait_for(broker.queue.get(), 2))
    assert message["task_id"] == "custom-task-id"
    assert message["task_name"] == "ping:pong"
    assert message["args"] == [1]
    assert message["kwargs"] == {"key": "value"}
    assert state["acks"][0]["results"][0]["status"] == "applied"


async def test_snapshot_pushed_after_oneoff_fires(
    admin_server: tuple[TestServer, AdminState],
) -> None:
    server, state = admin_server
    broker = AsyncQueueBroker()
    schedule = ScheduledTask(
        task_name="ping:pong",
        labels={},
        args=[],
        kwargs={},
        time=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    source = EditableScheduleSource([schedule])
    plugin, scheduler = make_scheduler(server, broker, [source])
    plugin._resolve_names()

    await scheduler.on_ready(source, schedule)

    # The one-off deleted itself on send, and the plugin's post_send
    # hook pushed a snapshot that no longer contains it.
    assert schedule.schedule_id not in source.schedules
    assert state["snapshots"]
    assert state["snapshots"][-1]["schedules"] == []


async def test_delete_command_fails_on_read_only_source(
    admin_server: tuple[TestServer, AdminState],
) -> None:
    server, state = admin_server
    plugin, _ = make_scheduler(server, AsyncQueueBroker(), [ReadOnlyScheduleSource()])
    plugin._resolve_names()
    state["commands"] = [
        {"id": "cmd-4", "type": "delete", "payload": {"schedule_id": "whatever"}},
    ]

    await plugin._poll_once()

    result = state["acks"][0]["results"][0]
    assert result["status"] == "failed"
    assert "does not support" in result["error"]


async def test_unavailable_admin_is_suppressed() -> None:
    source = EditableScheduleSource([get_schedule()])
    plugin = TaskiqAdminSchedulerPlugin(
        "http://127.0.0.1:1",
        "supersecret",
        timeout=1,
    )
    scheduler = TaskiqScheduler(AsyncQueueBroker(), [source], plugins=[plugin])
    plugin._resolve_names()

    await scheduler.on_schedules_updated(source, await source.get_schedules())
    await plugin._poll_once()
    await plugin.shutdown()
