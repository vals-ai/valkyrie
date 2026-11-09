import asyncio
from collections.abc import AsyncIterator, Awaitable
from typing import BinaryIO

import httpx

from valkyrie.sdk.errors import ValkyrieAPIError

CHUNK_SIZE = 1024 * 1024


async def file_chunks(stream: BinaryIO) -> AsyncIterator[bytes]:
    while chunk := await asyncio.to_thread(stream.read, CHUNK_SIZE):
        yield chunk


async def download(
    client: httpx.AsyncClient,
    url: str,
    output: BinaryIO,
    *,
    auth: httpx.Auth | None,
    max_bytes: float = float("inf"),
    too_large: str = "",
) -> int:
    size = 0
    async with client.stream("GET", url, auth=auth, follow_redirects=auth is None) as response:
        if response.is_error:
            await response.aread()
            raise ValkyrieAPIError.from_response(response)
        response.raise_for_status()
        async for chunk in response.aiter_bytes(chunk_size=CHUNK_SIZE):
            size += len(chunk)
            if size > max_bytes:
                raise ValueError(too_large)
            await asyncio.to_thread(output.write, chunk)
    return size


async def finish_before_cancelling[T](work: Awaitable[T]) -> T:
    task = asyncio.ensure_future(work)
    cancelled = False
    while not task.done():
        try:
            await asyncio.wait({task})
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        task.exception()
        raise asyncio.CancelledError
    return task.result()
