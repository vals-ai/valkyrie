import asyncio
import tempfile
import zipfile
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from typing import IO
from uuid import UUID

from tracker import config
from tracker.database.models import DatasetVersion
from tracker.repositories.dataset_version import DatasetVersionRepository
from tracker.runtime.artifacts import dataset_blob_key, dataset_file_key
from tracker.runtime.storage import STREAM_CHUNK_BYTES, ObjectStore
from tracker.services.datasets.digest import archive_digest
from tracker.services.datasets.extract import UnsafeArchiveError
from tracker.services.datasets.manifest import check_manifest
from tracker.services.datasets.metric_script import (
    MAX_METRIC_BYTES,
    InvalidMetricScriptError,
    check_metric_script,
    file_digest,
)
from tracker.services.datasets.validate import TaskReport, validate_archive
from tracker.types import VersionCreateRequest

ZIP_MAGIC = b"PK\x03\x04"
STORAGE_CONCURRENCY = 16

StorageGuard = Callable[[str], AbstractContextManager[None]]


def _unguarded(_noun: str) -> AbstractContextManager[None]:
    return nullcontext()


class UploadError(Exception):
    pass


class UploadTooLargeError(UploadError):
    pass


class InvalidUploadError(UploadError):
    pass


class RejectedUploadError(UploadError):
    pass


class NotStoredError(UploadError):
    pass


@dataclass(frozen=True)
class StoredUpload:
    size: int
    existed: bool
    warnings: list[str] = field(default_factory=list)


async def stored_sizes(objects: ObjectStore, dataset_id: UUID, digests: list[str]) -> dict[str, int]:
    limit = asyncio.Semaphore(STORAGE_CONCURRENCY)

    async def look(digest: str) -> tuple[str, int] | None:
        key = dataset_blob_key(str(dataset_id), digest)
        async with limit:
            if not await objects.exists(key):
                return None
            return digest, (await objects.stat(key)).size

    return dict(found for found in await asyncio.gather(*(look(digest) for digest in digests)) if found is not None)


async def _spool(body: AsyncIterator[bytes], spool: IO[bytes]) -> int:
    uploaded = 0
    async for chunk in body:
        uploaded += len(chunk)
        if uploaded > config.DATASET_TASK_UPLOAD_MAX_BYTES:
            raise UploadTooLargeError("Task upload exceeds DATASET_TASK_UPLOAD_MAX_BYTES")
        await asyncio.to_thread(spool.write, chunk)
    spool.seek(0)
    if spool.read(len(ZIP_MAGIC)) != ZIP_MAGIC:
        raise InvalidUploadError("Expected a zip archive")
    return uploaded


def _inspect(spool: IO[bytes]) -> tuple[str, TaskReport]:
    try:
        with zipfile.ZipFile(spool) as archive:
            return archive_digest(archive), validate_archive(archive)
    except zipfile.BadZipFile as error:
        raise InvalidUploadError("Expected a readable zip archive") from error
    except UnsafeArchiveError as error:
        raise InvalidUploadError(str(error)) from error


async def _chunks(spool: IO[bytes]) -> AsyncIterator[bytes]:
    spool.seek(0)
    while chunk := await asyncio.to_thread(spool.read, STREAM_CHUNK_BYTES):
        yield chunk


async def store_task_archive(
    objects: ObjectStore,
    dataset_id: UUID,
    digest: str,
    body: AsyncIterator[bytes],
    guard: StorageGuard = _unguarded,
) -> StoredUpload:
    key = dataset_blob_key(str(dataset_id), digest)
    with tempfile.TemporaryFile() as spool:
        size = await _spool(body, spool)
        actual, report = await asyncio.to_thread(_inspect, spool)
        if actual != digest:
            raise RejectedUploadError(f"The archive's files hash to {actual}, not {digest}")
        if not report.ok:
            raise RejectedUploadError(f"The task is not valid: {'; '.join(report.errors)}")
        with guard("Blob"):
            existed = await objects.exists(key)
            if not existed:
                await objects.put_stream(key, _chunks(spool))
    return StoredUpload(size=size, existed=existed, warnings=list(report.warnings))


async def store_metric_script(
    objects: ObjectStore,
    dataset_id: UUID,
    digest: str,
    body: AsyncIterator[bytes],
    guard: StorageGuard = _unguarded,
) -> StoredUpload:
    chunks: list[bytes] = []
    size = 0
    async for chunk in body:
        size += len(chunk)
        if size > MAX_METRIC_BYTES:
            raise UploadTooLargeError(f"The script is larger than {MAX_METRIC_BYTES} bytes")
        chunks.append(chunk)
    content = b"".join(chunks)
    actual = file_digest(content)
    if actual != digest:
        raise RejectedUploadError(f"The bytes hash to {actual}, not {digest}")
    try:
        check_metric_script(content)
    except InvalidMetricScriptError as error:
        raise InvalidUploadError(str(error)) from error
    key = dataset_file_key(str(dataset_id), digest)
    with guard("File"):
        existed = await objects.exists(key)
        if not existed:
            await objects.put_bytes(key, content)
    return StoredUpload(size=size, existed=existed)


async def publish_version(
    objects: ObjectStore,
    versions: DatasetVersionRepository,
    dataset_id: UUID,
    request: VersionCreateRequest,
    email: str | None,
    guard: StorageGuard = _unguarded,
) -> tuple[DatasetVersion, bool]:
    tasks, manifest = request.tasks, request.manifest
    if manifest is not None:
        check_manifest(tasks, manifest)
    if request.metric_digest is not None:
        with guard("File"):
            if not await objects.exists(dataset_file_key(str(dataset_id), request.metric_digest)):
                raise NotStoredError("metric.py is not stored yet; upload it first")
    digests = list(dict.fromkeys(tasks.values()))
    with guard("Blob"):
        sizes = await stored_sizes(objects, dataset_id, digests)
    absent = [digest for digest in digests if digest not in sizes]
    if absent:
        raise NotStoredError(f"{len(absent)} task archives are not stored yet, such as {absent[0]}; upload them first")
    sized = {task_id: (digest, sizes[digest]) for task_id, digest in tasks.items()}
    return await asyncio.to_thread(
        versions.publish, dataset_id, sized, request.tags, email, manifest, request.metric_digest
    )
