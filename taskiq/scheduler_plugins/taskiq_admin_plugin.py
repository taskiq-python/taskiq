import asyncio
import contextlib
import json
from datetime import datetime, timezone
from logging import getLogger
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import aiohttp

from taskiq.abc.schedule_source import ScheduleSource
from taskiq.abc.scheduler_plugin import SchedulerPlugin
from taskiq.compat import model_dump, model_validate
from taskiq.kicker import AsyncKicker
from taskiq.scheduler.scheduled_task import ScheduledTask

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncTaskiqDecoratedTask

__all__ = ("TaskiqAdminSchedulerPlugin",)

_logger = getLogger("taskiq.taskiq_admin_scheduler_plugin")


def _json_safe(value: Any) -> Any:
    """Return the value if it's JSON serializable, its repr otherwise."""
    try:
        json.dumps(value)
    except Exception:
        return repr(value)
    return value


def _is_editable_source(source: ScheduleSource) -> bool:
    """
    Check whether a source supports adding and deleting schedules.

    Sources that don't override the optional `add_schedule` and
    `delete_schedule` methods of the `ScheduleSource` base class
    (like the label based one) are read-only for the admin.
    """
    source_class = type(source)
    return (
        source_class.add_schedule is not ScheduleSource.add_schedule
        and source_class.delete_schedule is not ScheduleSource.delete_schedule
    )


def _dump_schedule(schedule: ScheduledTask) -> dict[str, Any]:
    """
    Serialize a schedule into a snapshot item.

    If the schedule's arguments cannot be serialized to JSON,
    they are replaced with their reprs and the item is marked
    as opaque, so the admin disables editing and triggering for it.
    """
    try:
        data = model_dump(schedule)
        opaque = False
    except Exception:
        opaque = True
        data = {
            "cron": schedule.cron,
            "args": [repr(arg) for arg in schedule.args],
            "kwargs": {key: repr(value) for key, value in schedule.kwargs.items()},
            "labels": {key: repr(value) for key, value in schedule.labels.items()},
            "cron_offset": (
                None if schedule.cron_offset is None else str(schedule.cron_offset)
            ),
            "time": None if schedule.time is None else schedule.time.isoformat(),
            "interval": None if schedule.interval is None else str(schedule.interval),
        }
    cron_offset = data.get("cron_offset")
    return {
        "scheduleId": schedule.schedule_id,
        "taskName": schedule.task_name,
        "cron": data.get("cron"),
        "cronOffset": None if cron_offset is None else str(cron_offset),
        "time": data.get("time"),
        "interval": data.get("interval"),
        "args": data.get("args") or [],
        "kwargs": data.get("kwargs") or {},
        "labels": data.get("labels") or {},
        "opaque": opaque,
    }


class TaskiqAdminSchedulerPlugin(SchedulerPlugin):
    """
    A scheduler plugin that syncs schedules with the taskiq-admin panel.

    The counterpart of `TaskiqAdminMiddleware` for the scheduler process.
    On every schedule refresh it pushes a snapshot of each source to the
    admin, on startup it reports all registered tasks, and in the
    background it polls the admin for commands created from its UI
    (delete, add, trigger) and applies them to the sources or the broker.

    All HTTP errors are logged and suppressed, so an unavailable
    admin never breaks scheduling.

    Attributes:
        url (str): Base URL of the admin API.
        api_token (str): Token used for authenticating with the API.
        timeout (int): Timeout (in seconds) for API requests.
        poll_interval (float): Delay between command polls, in seconds.
    """

    def __init__(
        self,
        url: str,
        api_token: str,
        timeout: int = 5,
        poll_interval: float = 2.0,
        source_names: dict[ScheduleSource, str] | None = None,
    ) -> None:
        super().__init__()
        self.url = url
        self.api_token = api_token
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._explicit_names = source_names or {}
        self._names: dict[int, str] = {}
        self._client: aiohttp.ClientSession | None = None
        self._poll_task: asyncio.Task[Any] | None = None

    def _get_client(self) -> aiohttp.ClientSession:
        """Create and cache session."""
        if self._client is None or self._client.closed:
            self._client = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            )
        return self._client

    async def _post(self, endpoint: str, payload: dict[str, Any]) -> Any:
        """
        Send a POST request to the admin.

        Returns the parsed response, or None if the request failed.
        """
        try:
            client = self._get_client()
            async with client.post(
                urljoin(self.url, endpoint),
                headers={"access-token": self.api_token},
                json=payload,
            ) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as exc:
            _logger.warning("Cannot reach taskiq-admin at %s: %s", self.url, exc)
            return None

    def _resolve_names(self) -> None:
        """Assign a unique name to every source of the scheduler."""
        used: set[str] = set()
        for source in self.scheduler.sources:
            name = self._explicit_names.get(source, type(source).__name__)
            deduplicated = name
            counter = 1
            while deduplicated in used:
                counter += 1
                deduplicated = f"{name}-{counter}"
            used.add(deduplicated)
            self._names[id(source)] = deduplicated

    def source_name(self, source: ScheduleSource) -> str:
        """
        Get the admin-facing name of a source.

        :param source: source to get the name of.
        :return: name of the source.
        """
        return self._names[id(source)]

    async def startup(self) -> None:
        """Report the broker's tasks and start the command polling loop."""
        self._resolve_names()
        await self._push_registered_tasks()
        self._poll_task = asyncio.get_event_loop().create_task(self._poll_loop())

    async def shutdown(self) -> None:
        """Stop the polling loop and close the session."""
        if self._poll_task is not None:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
        if self._client is not None:
            await self._client.close()

    async def on_schedules_updated(
        self,
        source: ScheduleSource,
        schedules: list[ScheduledTask],
    ) -> None:
        """
        Push a snapshot of the refreshed source to the admin.

        :param source: source the schedules were fetched from.
        :param schedules: all schedules the source returned.
        """
        await self._push_snapshot(source, schedules)

    async def post_send(
        self,
        source: ScheduleSource,
        task: ScheduledTask,
    ) -> None:
        """
        Push a fresh snapshot after a one-off schedule fires.

        One-off (time) schedules delete themselves from their source
        after firing, so an immediate snapshot keeps the admin from
        showing them as overdue until the next refresh.

        :param source: source that triggered this task.
        :param task: task that has been sent.
        """
        if task.time is None:
            return
        try:
            schedules = await source.get_schedules()
        except Exception:
            _logger.exception(
                "Cannot get schedules from source %s.",
                self.source_name(source),
            )
            return
        await self._push_snapshot(source, schedules)

    async def _push_snapshot(
        self,
        source: ScheduleSource,
        schedules: list[ScheduledTask],
    ) -> None:
        """Push the full list of schedules of a source to the admin."""
        await self._post(
            "/api/schedules/snapshot",
            {
                "sourceName": self.source_name(source),
                "editable": _is_editable_source(source),
                "scannedAt": datetime.now(timezone.utc)
                .replace(tzinfo=None)
                .isoformat(),
                "schedules": [_dump_schedule(schedule) for schedule in schedules],
            },
        )

    async def _push_registered_tasks(self) -> None:
        """
        Push all registered tasks of the broker to the admin.

        This lets the admin run or schedule any known task,
        even one that has no schedule yet.
        """
        tasks: dict[str, AsyncTaskiqDecoratedTask[Any, Any]] = (
            self.scheduler.broker.get_all_tasks()
        )
        await self._post(
            "/api/schedules/tasks-snapshot",
            {
                "tasks": [
                    {
                        "name": name,
                        "labels": {
                            key: _json_safe(value) for key, value in task.labels.items()
                        },
                    }
                    for name, task in tasks.items()
                ],
            },
        )

    async def _poll_loop(self) -> None:
        """Poll the admin for commands until cancelled."""
        while True:
            try:
                await self._poll_once()
            except Exception:
                _logger.exception("Cannot poll taskiq-admin for commands.")
            await asyncio.sleep(self.poll_interval)

    async def _poll_once(self) -> None:
        """Poll, apply and acknowledge commands for every source."""
        for source in self.scheduler.sources:
            response = await self._post(
                "/api/schedules/commands/poll",
                {"sourceName": self.source_name(source)},
            )
            commands = (response or {}).get("commands") or []
            if not commands:
                continue
            results = []
            for command in commands:
                status, error = await self._apply_command(command, source)
                results.append(
                    {"id": command["id"], "status": status, "error": error},
                )
            await self._post("/api/schedules/commands/ack", {"results": results})
            # An immediate snapshot, so the UI reflects
            # the applied commands right away.
            try:
                schedules = await source.get_schedules()
            except Exception:
                _logger.exception(
                    "Cannot get schedules from source %s.",
                    self.source_name(source),
                )
                continue
            await self._push_snapshot(source, schedules)

    async def _apply_command(
        self,
        command: dict[str, Any],
        source: ScheduleSource,
    ) -> tuple[str, str | None]:
        """
        Apply a single admin command against a source or the broker.

        Delete and add commands are applied to the source, trigger
        commands are kicked to the broker directly. A trigger command
        may carry a task_id to re-run an exact task.

        :param command: command received from the admin.
        :param source: source the command is scoped to.
        :return: tuple of resulting status and an optional error.
        """
        command_type = command.get("type")
        payload = command.get("payload") or {}
        editable = _is_editable_source(source)
        try:
            if command_type == "delete":
                if not editable:
                    return "failed", "Source does not support deleting schedules."
                await source.delete_schedule(payload["schedule_id"])
            elif command_type == "add":
                if not editable:
                    return "failed", "Source does not support adding schedules."
                await source.add_schedule(model_validate(ScheduledTask, payload))
            elif command_type == "trigger":
                kicker: AsyncKicker[Any, Any] = AsyncKicker(
                    payload["task_name"],
                    self.scheduler.broker,
                    payload.get("labels") or {},
                )
                if payload.get("task_id"):
                    kicker = kicker.with_task_id(payload["task_id"])
                await kicker.kiq(
                    *(payload.get("args") or []),
                    **(payload.get("kwargs") or {}),
                )
            else:
                return "failed", f"Unknown command type: {command_type}."
        except Exception as exc:
            _logger.exception("Cannot apply admin command %s.", command.get("id"))
            return "failed", repr(exc)
        return "applied", None
