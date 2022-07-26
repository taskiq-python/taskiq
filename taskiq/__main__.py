import asyncio

from taskiq.cli.args import TaskiqArgs
from taskiq.cli.worker import run_worker


def main() -> None:
    """Main entrypoint for CLI."""
    args = TaskiqArgs.from_cli()
    asyncio.run(run_worker(args))


if __name__ == "__main__":
    main()
