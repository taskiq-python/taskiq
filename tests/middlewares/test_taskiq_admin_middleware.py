import asyncio
from typing import AsyncGenerator
from unittest.mock import AsyncMock, Mock, patch

import pytest

from taskiq import TaskiqMessage
from taskiq.middlewares.taskiq_admin_middleware import TaskiqAdminMiddleware


@pytest.fixture
async def middleware() -> AsyncGenerator[TaskiqAdminMiddleware, None]:
    middleware = TaskiqAdminMiddleware(
        url="http://localhost:8000",
        api_token="test-token",  # noqa: S106
        timeout=5,
        taskiq_broker_name="test-broker",
    )
    await middleware.startup()
    yield middleware
    await middleware.shutdown()


@pytest.fixture
def message() -> TaskiqMessage:
    return TaskiqMessage(
        task_id="task-123",
        task_name="test_task",
        labels={},
        args=[1, 2, 3],
        kwargs={"key": "value"},
    )


def _make_mock_response() -> AsyncMock:
    """Create a properly configured mock response object."""
    mock_response = AsyncMock()
    mock_response.__aenter__.return_value = mock_response
    mock_response.__aexit__.return_value = None
    mock_response.ok = True
    mock_response.raise_for_status = Mock()
    return mock_response


class TestTaskiqAdminMiddlewarePostSend:
    async def test_when_post_send_is_called__then_queued_endpoint_is_called(
        self,
        middleware: TaskiqAdminMiddleware,
        message: TaskiqMessage,
    ) -> None:
        # Given
        with patch("aiohttp.ClientSession.post") as mock_post:
            mock_response = _make_mock_response()
            mock_post.return_value = mock_response

            # When
            await middleware.post_send(message)
            await asyncio.sleep(0.1)

            # Then
            mock_post.assert_called()
            assert mock_post.call_args is not None
            assert "/api/tasks/task-123/queued" in mock_post.call_args[0][0]

    async def test_when_post_send_is_called__then_payload_includes_task_info(
        self,
        middleware: TaskiqAdminMiddleware,
        message: TaskiqMessage,
    ) -> None:
        # Given
        with patch("aiohttp.ClientSession.post") as mock_post:
            mock_response = _make_mock_response()
            mock_post.return_value = mock_response

            # When
            await middleware.post_send(message)
            await asyncio.sleep(0.1)

            # Then
            call_args = mock_post.call_args
            assert call_args is not None
            payload = call_args[1]["json"]
            assert payload["args"] == [1, 2, 3]
            assert payload["kwargs"] == {"key": "value"}
            assert payload["taskName"] == "test_task"
            assert payload["worker"] == "test-broker"
            assert "queuedAt" in payload
