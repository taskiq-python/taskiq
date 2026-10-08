from pathlib import Path
from queue import Queue
from unittest.mock import Mock

import pytest
from watchdog import events
from watchdog.events import (
    FileClosedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    FileOpenedEvent,
    FileSystemEvent,
)
from watchdog.observers.api import BaseObserver, EventEmitter

from taskiq.cli.worker import process_manager
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.process_manager import ProcessManager, ReloadAllAction


@pytest.fixture(params=[[], ["first", "second"]], ids=["default", "multiple-dirs"])
def reloading_manager(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> tuple[ProcessManager, BaseObserver]:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(process_manager.signal, "signal", Mock())
    monkeypatch.setattr(process_manager, "Queue", Queue)
    reload_dirs = [str(tmp_path / name) for name in request.param]
    for directory in reload_dirs:
        Path(directory).mkdir()

    # Leave the observer stopped so dispatch cannot hide unwanted queued events.
    observer = BaseObserver(EventEmitter)
    manager = ProcessManager(
        WorkerArgs(
            broker="example:broker",
            modules=[],
            reload=True,
            reload_dirs=reload_dirs,
            no_gitignore=True,
        ),
        worker_function=Mock(),
        observer=observer,
    )
    assert {emitter.watch.path for emitter in observer.emitters} == set(
        reload_dirs or ["."],
    )
    assert all(emitter.watch.is_recursive for emitter in observer.emitters)
    return manager, observer


@pytest.mark.parametrize(
    "event_class",
    [
        FileOpenedEvent,
        FileClosedEvent,
        getattr(events, "FileClosedNoWriteEvent", None),
    ],
    ids=["opened", "closed", "closed-no-write"],
)
def test_open_and_close_events_never_enter_observer_queue(
    reloading_manager: tuple[ProcessManager, BaseObserver],
    event_class: type[FileSystemEvent] | None,
) -> None:
    if event_class is None:
        pytest.skip("This watchdog version does not emit read-only close events")

    manager, observer = reloading_manager
    for emitter in observer.emitters:
        for index in range(10):
            emitter.queue_event(event_class(f"task_{index}.py"))
        assert observer.event_queue.empty()
    assert manager.action_queue.empty()


@pytest.mark.parametrize(
    "event",
    [
        FileCreatedEvent("task.py"),
        FileModifiedEvent("task.py"),
        FileDeletedEvent("task.py"),
        FileMovedEvent("task.py", "renamed_task.py"),
    ],
    ids=["created", "modified", "deleted", "moved"],
)
def test_file_changes_queue_worker_reload(
    reloading_manager: tuple[ProcessManager, BaseObserver],
    event: FileSystemEvent,
) -> None:
    manager, observer = reloading_manager
    for emitter in observer.emitters:
        emitter.queue_event(event)
        assert not observer.event_queue.empty()
        observer.dispatch_events(observer.event_queue)
        assert isinstance(manager.action_queue.get_nowait(), ReloadAllAction)
        assert manager.action_queue.empty()
    assert observer.event_queue.empty()
