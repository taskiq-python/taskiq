"""Broker module used by test_hardkill_count subprocess tests.

The broker never yields messages and its graceful shutdown blocks on an
event that is never set, so a running worker can only exit via the
hard-kill path (KeyboardInterrupt raised from the signal handler).
"""

import asyncio
from collections.abc import AsyncGenerator

from taskiq.abc.broker import AsyncBroker
from taskiq.message import BrokerMessage


class HangingBroker(AsyncBroker):
    """Broker that idles forever and never finishes graceful shutdown."""

    async def kick(self, message: BrokerMessage) -> None:
        """No-op kick."""

    def listen(self) -> AsyncGenerator[bytes, None]:
        """Return an async generator that never yields."""

        async def _gen() -> AsyncGenerator[bytes, None]:
            await asyncio.Event().wait()
            yield b""  # pragma: no cover

        return _gen()

    async def shutdown(self) -> None:
        """Block forever, simulating a graceful shutdown that hangs."""
        await asyncio.Event().wait()


broker = HangingBroker()
