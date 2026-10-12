from taskiq.cli.scheduler.args import SchedulerArgs


def test_loop_factory_accepts_import_string() -> None:
    args = SchedulerArgs.from_cli(
        ["example:scheduler", "--loop-factory", "asyncio:SelectorEventLoop"],
    )

    assert args.loop_factory == "asyncio:SelectorEventLoop"
