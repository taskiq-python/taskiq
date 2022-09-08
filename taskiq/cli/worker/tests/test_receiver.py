from typing import Any, Optional

import pytest

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.receiver import Receiver
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.result import TaskiqResult


def get_receiver(
    broker: Optional[AsyncBroker] = None,
    no_parse: bool = False,
) -> Receiver:
    """
    Returns receiver with custom broker and args.

    :param broker: broker, defaults to None
    :param no_parse: parameter to taskiq_args, defaults to False
    :return: new receiver.
    """
    if broker is None:
        broker = InMemoryBroker()
    return Receiver(
        broker,
        WorkerArgs(
            broker="",
            modules=[],
            no_parse=no_parse,
        ),
    )


@pytest.mark.anyio
async def test_run_task_successfull_async() -> None:
    """Tests that run_task can run async tasks."""

    async def test_func(param: int) -> int:
        return param

    receiver = get_receiver()

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={},
            args=[1],
            kwargs={},
        ),
    )

    assert result.return_value == 1


@pytest.mark.anyio
async def test_run_task_successfull_sync() -> None:
    """Tests that run_task can run sync tasks."""

    def test_func(param: int) -> int:
        return param

    receiver = get_receiver()

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={},
            args=[1],
            kwargs={},
        ),
    )
    assert result.return_value == 1


@pytest.mark.anyio
async def test_run_task_exception() -> None:
    """Tests that run_task can run sync tasks."""

    def test_func() -> None:
        raise ValueError()

    receiver = get_receiver()

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={},
            args=[],
            kwargs={},
        ),
    )
    assert result.return_value is None
    assert result.is_err


@pytest.mark.anyio
async def test_run_task_exception_middlewares() -> None:
    """Tests that run_task can run sync tasks."""

    class _TestMiddleware(TaskiqMiddleware):
        found_exceptions = []

        def on_error(
            self,
            message: "TaskiqMessage",
            result: "TaskiqResult[Any]",
            exception: Exception,
        ) -> None:
            self.found_exceptions.append(exception)

    def test_func() -> None:
        raise ValueError()

    broker = InMemoryBroker()
    broker.add_middlewares([_TestMiddleware()])
    receiver = get_receiver(broker)

    result = await receiver.run_task(
        test_func,
        TaskiqMessage(
            task_id="",
            task_name="",
            labels={},
            args=[],
            kwargs={},
        ),
    )
    assert result.return_value is None
    assert result.is_err
    assert len(_TestMiddleware.found_exceptions) == 1
    assert _TestMiddleware.found_exceptions[0].__class__ == ValueError


@pytest.mark.anyio
async def test_callback_success() -> None:
    """Test that callback funcion works well."""
    broker = InMemoryBroker()
    called_times = 0

    @broker.task
    async def my_task() -> int:
        nonlocal called_times  # noqa: WPS420
        called_times += 1
        return 1

    receiver = get_receiver(broker)

    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name=my_task.task_name,
            labels={},
            args=[],
            kwargs=[],
        ),
    )

    await receiver.callback(broker_message)
    assert called_times == 1


@pytest.mark.anyio
async def test_callback_wrong_format() -> None:
    """Test that wrong format of a message won't thow an error."""
    receiver = get_receiver()

    await receiver.callback(
        BrokerMessage(
            task_id="",
            task_name="my_task.task_name",
            message='{"aaaa": "bbb"}',
            labels={},
        ),
    )


@pytest.mark.anyio
async def test_callback_unknown_task() -> None:
    """Tests that running an unknown task won't throw an error."""
    broker = InMemoryBroker()
    receiver = get_receiver(broker)

    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name="unknown",
            labels={},
            args=[],
            kwargs=[],
        ),
    )

    await receiver.callback(broker_message)
