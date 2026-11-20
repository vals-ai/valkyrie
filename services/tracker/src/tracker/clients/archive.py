import asyncio
import io
import time
from concurrent.futures import TimeoutError as FutureTimeoutError

from tracker.runtime.storage import ObjectStore

MAX_ARCHIVE_READ_BYTES = 32 * 1024 * 1024
MAX_ARCHIVE_READ_REQUESTS = 64
ARCHIVE_READ_SECONDS = 20


class ArchiveReadLimitError(Exception):
    pass


class ArchiveReadTimeoutError(Exception):
    pass


class ObjectRangeReader(io.RawIOBase):
    def __init__(self, objects: ObjectStore, key: str, size: int, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self._objects = objects
        self._key = key
        self._size = size
        self._loop = loop
        self._position = 0
        self._remaining = MAX_ARCHIVE_READ_BYTES
        self._requests = MAX_ARCHIVE_READ_REQUESTS
        self._deadline = time.monotonic() + ARCHIVE_READ_SECONDS

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            position = offset
        elif whence == io.SEEK_CUR:
            position = self._position + offset
        elif whence == io.SEEK_END:
            position = self._size + offset
        else:
            raise ValueError("Invalid seek origin")
        if position < 0:
            raise ValueError("Cannot seek before the archive")
        self._position = position

        return position

    async def _get_range(self, length: int, timeout: float) -> bytes:
        async with asyncio.timeout(timeout):
            return await self._objects.get_range(self._key, self._position, length)

    def read(self, size: int = -1) -> bytes:
        available = max(0, self._size - self._position)
        length = available if size < 0 else min(size, available)
        if length == 0:
            return b""
        timeout = self._deadline - time.monotonic()
        if timeout <= 0:
            raise ArchiveReadTimeoutError("Archive inspection timed out")
        if length > self._remaining or self._requests == 0:
            raise ArchiveReadLimitError("Archive inspection exceeds its read budget; download the task to inspect it")
        self._remaining -= length
        self._requests -= 1
        pending = asyncio.run_coroutine_threadsafe(self._get_range(length, timeout), self._loop)
        try:
            content = pending.result()
        except FutureTimeoutError as error:
            raise ArchiveReadTimeoutError("Archive inspection timed out") from error
        if len(content) != length:
            raise OSError("Storage returned an incomplete archive range")
        self._position += len(content)

        return content
