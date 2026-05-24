from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
from collections.abc import Sequence
from contextlib import suppress

from taskiq.abc.cmd import TaskiqCMD

from taskiq.brokers.nng.hub import HubConfig, NNGHub

logger = logging.getLogger(__name__)


class NNGHubCMD(TaskiqCMD):
    """Command to run the NNG hub."""

    short_help = "Run the NNG hub"

    def exec(self, args: Sequence[str]) -> int | None:
        parser = argparse.ArgumentParser(
            prog="taskiq nng-hub",
            description="NNG task router, dispatcher, and lease manager",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "--control-addr",
            default=os.getenv("NNG_CONTROL_ADDR", "ipc:///tmp/taskiq-nng.ipc"),
        )
        parser.add_argument(
            "--max-pending",
            type=int,
            default=int(os.getenv("NNG_MAX_PENDING", "10000")),
        )
        parser.add_argument(
            "--heartbeat-timeout",
            type=float,
            default=float(os.getenv("NNG_HEARTBEAT_TIMEOUT", "15.0")),
        )
        parser.add_argument(
            "--lease-timeout",
            type=float,
            default=float(os.getenv("NNG_LEASE_TIMEOUT", "20.0")),
        )
        parser.add_argument(
            "--routing-policy",
            choices=["least_loaded", "p2c", "round_robin"],
            default=os.getenv("NNG_ROUTING_POLICY", "least_loaded"),
        )
        parser.add_argument(
            "--control-concurrency",
            type=int,
            default=int(os.getenv("NNG_CONTROL_CONCURRENCY", "16")),
        )
        parser.add_argument(
            "--log-level",
            default=os.getenv("NNG_LOG_LEVEL", "INFO"),
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        )
        ns = parser.parse_args(list(args))
        logging.basicConfig(
            level=getattr(logging, ns.log_level),
            format="%(asctime)s %(name)-24s %(levelname)-8s %(message)s",
        )
        config = HubConfig(
            control_addr=ns.control_addr,
            max_pending=ns.max_pending,
            heartbeat_timeout=ns.heartbeat_timeout,
            lease_timeout=ns.lease_timeout,
            routing_policy=ns.routing_policy,
            control_concurrency=ns.control_concurrency,
        )
        asyncio.run(self._run(config))
        return 0

    async def _run(self, config: HubConfig) -> None:
        hub = NNGHub(config)
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def _on_signal() -> None:
            logger.info("Shutdown signal received")
            stop_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            with suppress(Exception):
                loop.add_signal_handler(sig, _on_signal)

        await hub.start()
        try:
            await stop_event.wait()
        finally:
            await hub.stop()
