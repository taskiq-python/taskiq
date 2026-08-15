import pytest

from taskiq.cli.worker.args import WorkerArgs


@pytest.mark.parametrize("max_prefetch", [0, 3])
def test_max_prefetch_accepts_non_negative_values(max_prefetch: int) -> None:
    args = WorkerArgs.from_cli(
        ["example:broker", "--max-prefetch", str(max_prefetch)],
    )

    assert args.max_prefetch == max_prefetch


def test_max_prefetch_rejects_negative_value(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        WorkerArgs.from_cli(
            ["example:broker", "--max-prefetch", "-1"],
        )

    assert exc_info.value.code == 2
    assert "max_prefetch cannot be negative" in capsys.readouterr().err


def test_max_prefetch_rejects_negative_default(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        WorkerArgs.from_cli(
            ["example:broker"],
            defaults={"max_prefetch": -1},
        )

    assert exc_info.value.code == 2
    assert "max_prefetch cannot be negative" in capsys.readouterr().err
