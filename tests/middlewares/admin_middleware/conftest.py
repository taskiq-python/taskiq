import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from typing_extensions import AsyncGenerator

from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.brokers.shared_broker import async_shared_broker
from taskiq.middlewares import TaskiqAdminMiddleware
from tests.middlewares.admin_middleware.dto import (
    DataclassDTO,
    PydanticDTO,
    TypedDictDTO,
)


@pytest.fixture
async def admin_api_server() -> AsyncGenerator[TestServer, None]:
    async def handle_queued(request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"}, status=200)

    async def handle_started(request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"}, status=200)

    async def handle_executed(request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"}, status=200)

    app = web.Application()
    app.router.add_post("/api/tasks/{task_id}/queued", handle_queued)
    app.router.add_post("/api/tasks/{task_id}/started", handle_started)
    app.router.add_post("/api/tasks/{task_id}/executed", handle_executed)

    server = TestServer(app)
    await server.start_server()
    yield server
    await server.close()


@pytest.fixture
async def broker_with_admin_middleware(
    admin_api_server: TestServer,
) -> AsyncGenerator[InMemoryBroker, None]:
    broker = InMemoryBroker(await_inplace=True).with_middlewares(
        TaskiqAdminMiddleware(
            str(admin_api_server.make_url("/")),  # URL тестового сервера
            "supersecret",
            taskiq_broker_name="InMemory",
        ),
    )

    broker.register_task(task_with_dataclass, task_name="task_with_dataclass")
    broker.register_task(task_with_typed_dict, task_name="task_with_typed_dict")
    broker.register_task(task_with_pydantic_model, task_name="task_with_pydantic_model")
    async_shared_broker.default_broker(broker)

    await broker.startup()
    yield broker
    await broker.shutdown()


async def task_with_dataclass(dto: DataclassDTO) -> None:
    assert dto


async def task_with_typed_dict(dto: TypedDictDTO) -> None:
    assert dto


async def task_with_pydantic_model(dto: PydanticDTO) -> None:
    assert dto
