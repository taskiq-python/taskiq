import asyncio

import anyio
import pytest

from taskiq.semaphore import PrioritySemaphore


@pytest.mark.anyio
async def test_semaphore_exception() -> None:
    with pytest.raises(ValueError):
        PrioritySemaphore(-1)


@pytest.mark.anyio
async def test_semaphore() -> None:
    sem = PrioritySemaphore(1)

    async def c1() -> None:
        await sem.acquire()

    async def c2() -> None:
        await sem.acquire()

    async def c3() -> None:
        await sem.acquire()

    t1 = asyncio.create_task(c1())
    t2 = asyncio.create_task(c2())
    t3 = asyncio.create_task(c3())
    await asyncio.sleep(0)

    sem.release()
    sem.release()
    t2.cancel()

    with anyio.fail_after(1):
        await asyncio.gather(t1, t2, t3, return_exceptions=True)
