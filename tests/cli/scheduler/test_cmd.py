import asyncio
from unittest.mock import patch

from taskiq.cli.scheduler.args import SchedulerArgs
from taskiq.cli.scheduler.cmd import SchedulerCMD


def test_scheduler_runs_on_configured_event_loop() -> None:
    parsed = SchedulerArgs(
        scheduler="example:scheduler",
        modules=[],
        loop_factory="asyncio:SelectorEventLoop",
    )
    running_loop: asyncio.AbstractEventLoop | None = None

    async def run_scheduler(_args: SchedulerArgs) -> None:
        nonlocal running_loop
        running_loop = asyncio.get_running_loop()

    with (
        patch.object(SchedulerArgs, "from_cli", return_value=parsed),
        patch("taskiq.cli.scheduler.cmd.run_scheduler", new=run_scheduler),
    ):
        SchedulerCMD().exec([])

    assert isinstance(running_loop, asyncio.SelectorEventLoop)


def test_scheduler_uses_default_event_loop_without_factory() -> None:
    parsed = SchedulerArgs(
        scheduler="example:scheduler",
        modules=[],
    )
    running_loop: asyncio.AbstractEventLoop | None = None

    async def run_scheduler(_args: SchedulerArgs) -> None:
        nonlocal running_loop
        running_loop = asyncio.get_running_loop()

    with (
        patch.object(SchedulerArgs, "from_cli", return_value=parsed),
        patch("taskiq.cli.scheduler.cmd.run_scheduler", new=run_scheduler),
        patch("taskiq.cli.scheduler.cmd.anyio.run") as anyio_run,
    ):
        SchedulerCMD().exec([])

    assert running_loop is not None
    anyio_run.assert_not_called()
