import asyncio
from datetime import datetime, timezone
from logging import getLogger
from typing import Any
from urllib.parse import urljoin

import aiohttp

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

__all__ = ("TaskiqAdminMiddleware",)

_logger = getLogger("taskiq.taskiq_admin_middleware")


class TaskiqAdminMiddleware(TaskiqMiddleware):
    """A Taskiq middleware that reports task lifecycle events to an external admin API.

    This middleware sends HTTP POST requests to a configured endpoint when tasks
    are queued, started, or completed. It can be used for task monitoring, auditing,
    or visualization in external systems.

    Attributes:
        url (str): Base URL of the admin API.
        api_token (str): Token used for authenticating with the API.
        timeout (int): Timeout (in seconds) for API requests.
        taskiq_broker_name (str | None): Optional name of the broker instance to include
            in the payload.
        _pending (set[asyncio.Task]): Set of currently running background request tasks.
        _client (aiohttp.ClientSession | None): HTTP client session used
            for sending requests.
    """

    def __init__(
        self,
        url: str,
        api_token: str,
        timeout: int = 5,
        taskiq_broker_name: str | None = None,
    ) -> None:
        super().__init__()
        self.url = url
        self.timeout = timeout
        self.api_token = api_token
        self.__ta_broker_name = taskiq_broker_name
        self._pending: set[asyncio.Task[Any]] = set()
        self._client: aiohttp.ClientSession | None = None

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    def _get_client(self) -> aiohttp.ClientSession:
        """Create and cache session."""
        if self._client is None or self._client.closed:
            self._client = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            )

        return self._client

    async def startup(self) -> None:
        """
        Startup method to initialize aiohttp.ClientSession.

        :returns nothing.
        """
        self._client = self._get_client()

    async def shutdown(self) -> None:
        """Shutdown method to run all pending requests and close the session.

        :returns nothing.
        """
        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)
        if self._client is not None:
            await self._client.close()

    async def _spawn_request(
        self,
        endpoint: str,
        payload: dict[str, Any],
    ) -> None:
        """Fire and forget helper.

        start an async POST to the admin API, keep the resulting Task in _pending
        so it can be awaited/cleaned during graceful shutdown.
        """

        async def _send() -> None:
            client = self._get_client()

            async with client.post(
                urljoin(self.url, endpoint),
                headers={"access-token": self.api_token},
                json=payload,
            ) as resp:
                resp.raise_for_status()
                if not resp.ok:
                    _logger.error(f"POST {endpoint} - {resp.status}")

        task = asyncio.create_task(_send())
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def post_send(self, message: TaskiqMessage) -> None:
        """
        This hook is executed right after the task is sent.

        This is a client-side hook. It executes right
        after the messages is kicked in broker.

        :param message: kicked message.
        """
        await self._spawn_request(
            f"/api/tasks/{message.task_id}/queued",
            {
                "args": message.args,
                "kwargs": message.kwargs,
                "labels": message.labels,
                "queuedAt": self._now_iso(),
                "taskName": message.task_name,
                "worker": self.__ta_broker_name,
            },
        )

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        """
        This hook is called before executing task.

        This is a worker-side hook, which means it
        executes in the worker process.

        :param message: incoming parsed taskiq message.
        :return: modified message.
        """
        await self._spawn_request(
            f"/api/tasks/{message.task_id}/started",
            {
                "args": message.args,
                "kwargs": message.kwargs,
                "labels": message.labels,
                "startedAt": self._now_iso(),
                "taskName": message.task_name,
                "worker": self.__ta_broker_name,
            },
        )
        return message

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        """
        This hook executes after task is complete.

        This is a worker-side hook. It's called
        in worker process.

        :param message: incoming message.
        :param result: result of execution for current task.
        """
        await self._spawn_request(
            f"/api/tasks/{message.task_id}/executed",
            {
                "finishedAt": self._now_iso(),
                "executionTime": result.execution_time,
                "error": None if result.error is None else repr(result.error),
                "returnValue": {"return_value": result.return_value},
            },
        )
