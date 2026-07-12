from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import pytest


@pytest.fixture
def runtime_executor() -> Iterator[ThreadPoolExecutor]:
    """Provide the process-wide sync executor owned by a worker runtime."""
    with ThreadPoolExecutor(max_workers=2) as executor:
        yield executor
