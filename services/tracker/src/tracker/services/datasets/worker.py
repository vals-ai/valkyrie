import asyncio
import tempfile
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import aclosing
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar
from uuid import UUID

from sqlalchemy import Engine
from sqlmodel import Session

from tracker.database.models import DatasetUpload
from tracker.database.session import database_call
from tracker.domain import DatasetUploadStatus
from tracker.exceptions import DatasetUploadConflictError
from tracker.repositories.dataset_upload import DatasetUploadRepository
from tracker.runtime.artifacts import dataset_blob_key, dataset_upload_key
from tracker.runtime.lifecycle import finish_cleanup
from tracker.runtime.storage import STREAM_CHUNK_BYTES, ObjectStore
from tracker.services.datasets.publish import (
    UploadTooLargeError,
    store_metric_script,
    store_task_blob,
    verify_version_objects,
)
from tracker.services.datasets.uploads import prepare_dataset_archive

_Result = TypeVar("_Result")
_HEARTBEAT_SECONDS = 30


@dataclass(frozen=True)
class UploadWork:
    id: UUID
    dataset_id: UUID
    lease_id: UUID
    status: DatasetUploadStatus
    archive_size: int
    archive_sha256: str
    target_name: str

    @classmethod
    def from_row(cls, upload: DatasetUpload, target_name: str) -> "UploadWork":
        if upload.lease_id is None or upload.status not in (
            DatasetUploadStatus.VALIDATING,
            DatasetUploadStatus.PUBLISHING,
        ):
            raise DatasetUploadConflictError("The upload has no worker claim")
        return cls(
            upload.id,
            upload.dataset_id,
            upload.lease_id,
            upload.status,
            upload.archive_size,
            upload.archive_sha256,
            target_name,
        )


async def with_upload_repository(engine: Engine, action: Callable[[DatasetUploadRepository], _Result]) -> _Result:
    def run() -> _Result:
        with Session(engine, expire_on_commit=False) as session:
            return action(DatasetUploadRepository(session))

    return await database_call(run)


async def _download(objects: ObjectStore, key: str, destination: Path, expected_size: int) -> None:
    downloaded = 0
    stream = objects.get_stream(key)
    try:
        with destination.open("xb") as sink:
            async for chunk in stream:
                downloaded += len(chunk)
                if downloaded > expected_size:
                    raise UploadTooLargeError("The uploaded archive exceeds its declared size")
                await finish_cleanup(asyncio.create_task(asyncio.to_thread(sink.write, chunk)))
    finally:
        if isinstance(stream, AsyncGenerator):
            await stream.aclose()


async def _chunks(path: Path) -> AsyncGenerator[bytes, None]:
    with path.open("rb") as source:
        while chunk := await finish_cleanup(asyncio.create_task(asyncio.to_thread(source.read, STREAM_CHUNK_BYTES))):
            yield chunk


async def _content(content: bytes) -> AsyncIterator[bytes]:
    yield content


async def _validate(engine: Engine, objects: ObjectStore, work: UploadWork, scratch_root: Path | None) -> None:
    with tempfile.TemporaryDirectory(prefix="dataset-upload-", dir=scratch_root) as scratch:
        root = Path(scratch)
        archive = root / "archive.zip"
        await _download(objects, dataset_upload_key(str(work.dataset_id), str(work.id)), archive, work.archive_size)
        prepared = await finish_cleanup(
            asyncio.create_task(
                asyncio.to_thread(
                    prepare_dataset_archive,
                    archive,
                    root / "prepared",
                    expected_size=work.archive_size,
                    expected_sha256=work.archive_sha256,
                    target_name=work.target_name,
                )
            )
        )
        sizes: dict[str, int] = {}
        for digest, path in prepared.archives.items():
            key = dataset_blob_key(str(work.dataset_id), digest)
            async with aclosing(_chunks(path)) as chunks:
                sizes[digest], _ = await store_task_blob(objects, key, chunks)
        if prepared.metric is not None:
            assert prepared.request.metric_digest is not None
            await store_metric_script(
                objects, work.dataset_id, prepared.request.metric_digest, _content(prepared.metric)
            )
        await with_upload_repository(
            engine,
            lambda repository: repository.complete_upload(
                work.id,
                work.lease_id,
                prepared.request,
                sizes,
                prepared.warnings,
            ),
        )


async def _process(engine: Engine, objects: ObjectStore, work: UploadWork, scratch_root: Path | None) -> None:
    if work.status == DatasetUploadStatus.PUBLISHING:
        request = await with_upload_repository(
            engine, lambda repository: repository.publication_request(work.id, work.lease_id)
        )
        sizes = await verify_version_objects(objects, work.dataset_id, request)
        await with_upload_repository(
            engine, lambda repository: repository.publish_upload(work.id, work.lease_id, request, sizes)
        )
    elif work.status == DatasetUploadStatus.VALIDATING:
        await _validate(engine, objects, work, scratch_root)
    else:
        raise DatasetUploadConflictError("The upload is not claimed for processing")


async def _heartbeat(engine: Engine, work: UploadWork) -> None:
    while True:
        await asyncio.sleep(_HEARTBEAT_SECONDS)
        if not await with_upload_repository(engine, lambda repository: repository.renew_upload(work.id, work.lease_id)):
            raise DatasetUploadConflictError("This worker no longer owns the upload")


async def process_upload(
    engine: Engine, objects: ObjectStore, work: UploadWork, *, scratch_root: Path | None = None
) -> None:
    if not await with_upload_repository(engine, lambda repository: repository.renew_upload(work.id, work.lease_id)):
        raise DatasetUploadConflictError("This worker no longer owns the upload")
    processing = asyncio.create_task(_process(engine, objects, work, scratch_root))
    heartbeat = asyncio.create_task(_heartbeat(engine, work))
    try:
        finished, _ = await asyncio.wait({processing, heartbeat}, return_when=asyncio.FIRST_COMPLETED)
        if heartbeat in finished:
            await heartbeat
        await processing
    finally:
        processing.cancel()
        heartbeat.cancel()
        await asyncio.gather(processing, heartbeat, return_exceptions=True)
