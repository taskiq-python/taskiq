from taskiq import Context, InMemoryBroker, TaskiqDepends
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage


async def test_requeue() -> None:
    broker = InMemoryBroker()

    runs_count = 0

    @broker.task
    async def task(context: Context = TaskiqDepends()) -> None:
        nonlocal runs_count
        runs_count += 1
        if runs_count < 2:
            await context.requeue()

    kicked = await task.kiq()
    await kicked.wait_result()
    assert (
        broker.custom_dependency_context[Context].message.labels["X-Taskiq-requeue"]
        == "1"
    )

    assert runs_count == 2


async def test_requeue_from_dependency() -> None:
    broker = InMemoryBroker()

    runs_count = 0

    async def dep_func(context: Context = TaskiqDepends()) -> None:
        nonlocal runs_count
        runs_count += 1
        if runs_count < 2:
            await context.requeue()

    @broker.task
    async def task(_: None = TaskiqDepends(dep_func)) -> None:
        return None

    kicked = await task.kiq()
    await kicked.wait_result()
    assert (
        broker.custom_dependency_context[Context].message.labels["X-Taskiq-requeue"]
        == "1"
    )

    assert runs_count == 2


async def test_requeue_triggers_send_middlewares() -> None:
    broker = InMemoryBroker()
    runs_count = 0

    class CountingMiddleware(TaskiqMiddleware):
        def __init__(self) -> None:
            super().__init__()
            self.pre_send_calls = 0
            self.post_send_calls = 0

        def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
            self.pre_send_calls += 1
            return message

        def post_send(self, message: TaskiqMessage) -> None:
            self.post_send_calls += 1

    middleware = CountingMiddleware()
    broker.add_middlewares(middleware)

    @broker.task
    async def task(context: Context = TaskiqDepends()) -> None:
        nonlocal runs_count
        runs_count += 1
        if runs_count < 2:
            await context.requeue()

    kicked = await task.kiq()
    await kicked.wait_result()

    assert middleware.pre_send_calls == 2
    assert middleware.post_send_calls == 2
