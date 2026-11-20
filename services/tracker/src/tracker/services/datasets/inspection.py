import asyncio
import codecs
import zipfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

from tracker.clients.archive import ObjectRangeReader
from tracker.runtime.lifecycle import finish_cleanup
from tracker.runtime.storage import ObjectStore
from tracker.services.datasets.extract import UnsafeArchiveError, check_archive
from tracker.types import DatasetFile, DatasetFilePreview, DatasetFilesPage

MAX_PREVIEW_BYTES = 64 * 1024
_T = TypeVar("_T")
_ARCHIVE_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="dataset-inspection")


async def inspect_archive(objects: ObjectStore, key: str, size: int, operation: Callable[[zipfile.ZipFile], _T]) -> _T:
    loop = asyncio.get_running_loop()
    reader = ObjectRangeReader(objects, key, size, loop)

    def inspect() -> _T:
        with reader, zipfile.ZipFile(reader) as archive:
            check_archive(archive)
            names = [entry.filename for entry in archive.infolist() if not entry.is_dir()]
            if len(names) != len(set(names)):
                raise UnsafeArchiveError("The archive contains duplicate file paths")

            return operation(archive)

    async def run() -> _T:
        return await loop.run_in_executor(_ARCHIVE_EXECUTOR, inspect)

    return await finish_cleanup(asyncio.create_task(run()))


def files_page(archive: zipfile.ZipFile, cursor: str | None, limit: int) -> DatasetFilesPage:
    entries = sorted(
        (entry for entry in archive.infolist() if not entry.is_dir() and (cursor is None or entry.filename > cursor)),
        key=lambda entry: entry.filename,
    )
    page = entries[:limit]

    return DatasetFilesPage(
        files=[
            DatasetFile(path=entry.filename, size=entry.file_size, mode=(entry.external_attr >> 16) & 0o777)
            for entry in page
        ],
        next_cursor=page[-1].filename if len(entries) > limit else None,
    )


def file_preview(archive: zipfile.ZipFile, path: str) -> DatasetFilePreview:
    try:
        entry = archive.getinfo(path)
    except KeyError as error:
        raise FileNotFoundError("File not found in this task") from error
    if entry.is_dir():
        raise FileNotFoundError("File not found in this task")
    with archive.open(entry) as stream:
        content = stream.read(MAX_PREVIEW_BYTES + 1)
    truncated = len(content) > MAX_PREVIEW_BYTES
    content = content[:MAX_PREVIEW_BYTES]
    text: str | None = None
    if b"\x00" not in content:
        try:
            text = codecs.getincrementaldecoder("utf-8")().decode(content, final=not truncated)
        except UnicodeDecodeError:
            pass

    return DatasetFilePreview(
        path=entry.filename, size=entry.file_size, content=text, truncated=truncated, binary=text is None
    )
