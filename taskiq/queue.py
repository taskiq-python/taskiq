from __future__ import annotations

import dataclasses

DEFAULT_QUEUE = "taksiq"


@dataclasses.dataclass(frozen=True, init=False, eq=True)
class Queue:
    """Represents an abstraction for dealing with queues in real brokers."""

    name: str

    def __init__(self, src: str | Queue) -> None:
        if isinstance(src, Queue):
            object.__setattr__(self, "name", src.name)
        elif isinstance(src, str):
            object.__setattr__(self, "name", src)
        else:
            raise TypeError(
                "Queue.__init__ expect str or Queue, "
                "{type(src).__name__!r} is recieved",
            )

    def __repr__(self) -> str:
        return self.name
