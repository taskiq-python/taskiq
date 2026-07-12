import asyncio
import logging
from concurrent.futures import Executor

import pytest

from taskiq import InMemoryBroker, TaskiqRouter
from taskiq.acks import AcknowledgeType
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.run import get_broker_selection, shutdown_broker, start_listen
from taskiq.receiver import Receiver
from taskiq.receiver.runtime import BrokerSelection, WorkerRuntimeOptions
from taskiq.warnings import TaskiqDeprecationWarning


def test_get_broker_selection_preserves_direct_single_broker() -> None:
    broker = InMemoryBroker()

    selection = get_broker_selection(WorkerArgs(broker=broker, modules=[]))

    assert selection.listeners == (broker,)
    assert not selection.explicit_group


def test_start_listen_builds_runtime_from_worker_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = InMemoryBroker()
    args = WorkerArgs(
        broker=broker,
        modules=["application.tasks"],
        tasks_pattern=("**/jobs.py",),
        fs_discover=True,
        configure_logging=False,
        max_threadpool_threads=2,
        no_parse=True,
        shutdown_timeout=0.7,
        max_async_tasks=7,
        max_async_tasks_jitter=2,
        receiver_arg=[("dependency", "value")],
        max_prefetch=3,
        no_propagate_errors=True,
        ack_type=AcknowledgeType.WHEN_RECEIVED,
        max_tasks_per_child=11,
        wait_tasks_timeout=0.5,
    )
    loop = asyncio.new_event_loop()
    captured_selection: BrokerSelection | None = None
    captured_receiver_type: type[Receiver] | None = None
    captured_executor: Executor | None = None
    captured_options: WorkerRuntimeOptions | None = None
    runtime_ran = False
    import_calls: list[tuple[list[str], tuple[str, ...], bool]] = []

    class RecordingRuntime:
        def __init__(
            self,
            *,
            selection: BrokerSelection,
            receiver_type: type[Receiver],
            executor: Executor,
            options: WorkerRuntimeOptions,
        ) -> None:
            nonlocal captured_selection
            nonlocal captured_receiver_type
            nonlocal captured_executor
            nonlocal captured_options
            captured_selection = selection
            captured_receiver_type = receiver_type
            captured_executor = executor
            captured_options = options

        async def run(self, finish_event: asyncio.Event) -> None:
            nonlocal runtime_ran
            runtime_ran = True
            finish_event.set()

    def record_import_tasks(
        modules: list[str],
        task_pattern: tuple[str, ...],
        fs_discover: bool,
    ) -> None:
        import_calls.append((modules, task_pattern, fs_discover))

    monkeypatch.setattr("taskiq.cli.worker.run.uvloop", None)
    monkeypatch.setattr("taskiq.cli.worker.run.signal.signal", lambda *args: None)
    monkeypatch.setattr("taskiq.cli.worker.run.asyncio.new_event_loop", lambda: loop)
    monkeypatch.setattr("taskiq.cli.worker.run.import_tasks", record_import_tasks)
    monkeypatch.setattr(
        "taskiq.cli.worker.run.get_receiver_type",
        lambda args: Receiver,
    )
    monkeypatch.setattr("taskiq.cli.worker.run.WorkerRuntime", RecordingRuntime)

    try:
        start_listen(args)
    finally:
        asyncio.set_event_loop(None)
        loop.close()

    assert captured_selection is not None
    assert captured_selection.listeners == (broker,)
    assert broker.is_worker_process
    assert captured_receiver_type is Receiver
    assert captured_executor is not None
    assert captured_options == WorkerRuntimeOptions(
        validate_params=False,
        max_async_tasks=7,
        max_async_tasks_jitter=2,
        max_prefetch=3,
        propagate_exceptions=False,
        ack_type=AcknowledgeType.WHEN_RECEIVED,
        max_tasks_to_execute=11,
        wait_tasks_timeout=0.5,
        shutdown_timeout=0.7,
        receiver_kwargs={"dependency": "value"},
    )
    assert import_calls == [(["application.tasks"], ("**/jobs.py",), True)]
    assert runtime_ran


@pytest.mark.parametrize("use_factory", [False, True])
def test_get_broker_selection_preserves_imported_single_broker(
    monkeypatch: pytest.MonkeyPatch,
    use_factory: bool,
) -> None:
    broker = InMemoryBroker()
    imported_source: object = broker
    if use_factory:

        def broker_factory() -> InMemoryBroker:
            return broker

        imported_source = broker_factory

    def fake_import_object(object_spec: str, app_dir: str | None = None) -> object:
        assert object_spec == "application.worker:broker"
        assert app_dir is None
        return imported_source

    monkeypatch.setattr("taskiq.cli.worker.run.import_object", fake_import_object)

    selection = get_broker_selection(
        WorkerArgs(broker="application.worker:broker", modules=[]),
    )

    assert selection.listeners == (broker,)
    assert not selection.explicit_group


async def test_legacy_shutdown_broker_helper_is_deprecated_and_redacted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    diagnostic = "sensitive-shutdown-diagnostic"

    class DiagnosticBroker(InMemoryBroker):
        async def shutdown(self) -> None:
            await super().shutdown()
            return diagnostic  # type: ignore[return-value]

    broker = DiagnosticBroker()
    await broker.startup()
    caplog.set_level(logging.INFO, logger="taskiq.worker.runtime")

    with pytest.warns(TaskiqDeprecationWarning, match="WorkerRuntime"):
        await shutdown_broker(broker, timeout=1)

    assert "returned a diagnostic value" in caplog.text
    assert diagnostic not in caplog.text


async def test_legacy_shutdown_broker_helper_preserves_cancellation() -> None:
    shutdown_error = asyncio.CancelledError("adapter shutdown cancelled")

    class CancelledShutdownBroker(InMemoryBroker):
        async def shutdown(self) -> None:
            raise shutdown_error

    broker = CancelledShutdownBroker()

    with (
        pytest.warns(
            TaskiqDeprecationWarning,
            match="WorkerRuntime",
        ),
        pytest.raises(asyncio.CancelledError),
    ):
        await shutdown_broker(broker, timeout=1)


@pytest.mark.parametrize("use_factory", [False, True])
def test_get_broker_selection_imports_explicit_listener_sequence(
    monkeypatch: pytest.MonkeyPatch,
    use_factory: bool,
) -> None:
    router = TaskiqRouter()
    first = InMemoryBroker(router=router, broker_name="first")
    second = InMemoryBroker(router=router, broker_name="second")
    listeners = (first, second)
    imported_source: object = listeners
    if use_factory:

        def listener_factory() -> tuple[InMemoryBroker, InMemoryBroker]:
            return listeners

        imported_source = listener_factory

    import_calls: list[tuple[str, str | None]] = []

    def fake_import_object(object_spec: str, app_dir: str | None = None) -> object:
        import_calls.append((object_spec, app_dir))
        return imported_source

    monkeypatch.setattr(
        "taskiq.cli.worker.run.import_object",
        fake_import_object,
    )

    selection = get_broker_selection(
        WorkerArgs(
            broker="application.worker:listeners",
            modules=[],
            app_dir="/application",
        ),
    )

    assert selection.listeners == listeners
    assert selection.explicit_group
    assert import_calls == [("application.worker:listeners", "/application")]
