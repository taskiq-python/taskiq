import logging

import pytest

from taskiq import InMemoryBroker, TaskiqRouter
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.run import get_broker_selection, shutdown_broker
from taskiq.warnings import TaskiqDeprecationWarning


def test_get_broker_selection_preserves_direct_single_broker() -> None:
    broker = InMemoryBroker()

    selection = get_broker_selection(WorkerArgs(broker=broker, modules=[]))

    assert selection.listeners == (broker,)
    assert not selection.explicit_group


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
