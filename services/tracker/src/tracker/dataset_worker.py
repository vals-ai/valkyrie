import argparse
import asyncio
import os
import signal
import sys
import tempfile
import zipfile
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from botocore.exceptions import ClientError
from sqlalchemy import Engine

from tracker.aws.resolver import deployment_aws_runtime
from tracker.aws.s3 import S3ObjectStore
from tracker.database.models import DatasetUpload
from tracker.database.session import engine
from tracker.exceptions import DatasetUploadConflictError, ResourceArchivedError, TagExistsError
from tracker.local import config as local_config
from tracker.local.storage import FilesystemObjectStore
from tracker.logging import configure_logging, get_logger
from tracker.runtime.artifacts import dataset_upload_key
from tracker.runtime.lifecycle import finish_cleanup
from tracker.runtime.storage import ObjectStore
from tracker.services.datasets.publish import UploadError
from tracker.services.datasets.worker import UploadWork, process_upload, with_upload_repository
from tracker.types import OrgContext

logger = get_logger(__name__)
_POLL_SECONDS = 5
_CLEANUP_SECONDS = 30


def _objects(org: OrgContext) -> ObjectStore:
    if local_config.resources is not None:
        root = local_config.resources.data_root / "orgs" / str(org.id)
        return FilesystemObjectStore(root / "objects", root / "staging")
    return S3ObjectStore(deployment_aws_runtime(org))


def _failure(error: Exception) -> tuple[str, bool]:
    if isinstance(error, (UploadError, ValueError, zipfile.BadZipFile, ResourceArchivedError, TagExistsError)):
        return str(error)[:2000], False
    if isinstance(error, FileNotFoundError):
        return "The uploaded archive is no longer available", False
    cause = error if isinstance(error, ClientError) else error.__cause__
    if isinstance(cause, ClientError) and cause.response.get("Error", {}).get("Code") in (
        "403",
        "AccessDenied",
        "Forbidden",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
    ):
        return "Storage access was denied", False
    return "Archive processing failed because a worker or dependency was unavailable", True


async def _fail(database: Engine, upload_id: UUID, lease_id: UUID, message: str, retryable: bool) -> None:
    try:
        await with_upload_repository(
            database, lambda repository: repository.fail_upload(upload_id, lease_id, message, retryable=retryable)
        )
    except DatasetUploadConflictError:
        logger.info("Upload %s no longer belongs to this worker", upload_id)


async def execute_upload(database: Engine, upload_id: UUID, lease_id: UUID, scratch_root: Path | None = None) -> None:
    try:
        upload = await with_upload_repository(
            database, lambda repository: repository.claimed_upload(upload_id, lease_id)
        )
        org, target_name = await with_upload_repository(
            database, lambda repository: repository.upload_storage(upload.dataset_id)
        )
        await process_upload(
            database, _objects(org), UploadWork.from_row(upload, target_name), scratch_root=scratch_root
        )
    except DatasetUploadConflictError:
        logger.info("Upload %s no longer belongs to this worker", upload_id)
    except Exception as error:
        logger.exception("Upload %s failed", upload_id)
        message, retryable = _failure(error)
        await _fail(database, upload_id, lease_id, message, retryable)


async def supervise_process(command: Sequence[str], timeout: float, scratch: str) -> int:
    creation = asyncio.create_task(asyncio.create_subprocess_exec(*command, env={**os.environ, "TMPDIR": scratch}))

    async def reap() -> None:
        try:
            process = await creation
        except Exception:
            return
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        await process.wait()

    try:
        async with asyncio.timeout(timeout):
            process = await asyncio.shield(creation)
            return await process.wait()
    finally:
        await finish_cleanup(asyncio.create_task(reap()))


async def run_claimed_upload(database: Engine, upload: DatasetUpload, configuration: Path | None = None) -> None:
    if upload.lease_id is None or upload.processing_deadline is None:
        raise ValueError("The upload has no processing lease")
    deadline = upload.processing_deadline
    if deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=UTC)
    remaining = (deadline - datetime.now(UTC)).total_seconds()
    if remaining <= 0:
        await with_upload_repository(database, lambda repository: repository.expire_uploads())
        return
    with tempfile.TemporaryDirectory(prefix="dataset-worker-") as scratch:
        command = [
            sys.executable,
            "-m",
            "tracker.dataset_worker",
            "--upload-id",
            str(upload.id),
            "--lease-id",
            str(upload.lease_id),
            "--scratch",
            scratch,
        ]
        if configuration is not None:
            command.extend(("--config", str(configuration.resolve())))
        try:
            return_code = await supervise_process(command, remaining, scratch)
        except TimeoutError:
            logger.error("Upload %s exceeded its processing deadline", upload.id)
            await with_upload_repository(database, lambda repository: repository.expire_uploads())
            return
        if return_code != 0:
            logger.error("Upload %s worker exited with status %s", upload.id, return_code)
            await _fail(database, upload.id, upload.lease_id, "The archive worker stopped unexpectedly", True)


async def clean_uploads(database: Engine) -> None:
    uploads = await with_upload_repository(database, lambda repository: repository.claim_upload_cleanup())
    for upload in uploads:
        try:
            org, _ = await with_upload_repository(
                database, lambda repository: repository.upload_storage(upload.dataset_id)
            )
            async with asyncio.timeout(_CLEANUP_SECONDS):
                try:
                    await _objects(org).delete(dataset_upload_key(str(upload.dataset_id), str(upload.id)))
                except FileNotFoundError:
                    pass
            await with_upload_repository(database, lambda repository: repository.finish_upload_cleanup(upload.id))
        except Exception:
            logger.exception("Upload %s temporary archive cleanup failed", upload.id)


async def run_worker(database: Engine, configuration: Path | None = None) -> None:
    while True:
        try:
            await with_upload_repository(database, lambda repository: repository.expire_uploads())
            await clean_uploads(database)
            upload = await with_upload_repository(database, lambda repository: repository.claim_upload())
            if upload is not None:
                await run_claimed_upload(database, upload, configuration)
        except Exception:
            logger.exception("Dataset worker iteration failed")
        await asyncio.sleep(_POLL_SECONDS)


async def _main(
    upload_id: UUID | None, lease_id: UUID | None, scratch: Path | None, configuration: Path | None
) -> None:
    task = asyncio.current_task()
    assert task is not None
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        if upload_id is not None and lease_id is not None:
            await execute_upload(engine, upload_id, lease_id, scratch)
        else:
            await run_worker(engine, configuration)
    except asyncio.CancelledError:
        pass
    finally:
        engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Process queued dataset archives and remove temporary uploads.")
    parser.add_argument("--config", type=Path, help="Local storage resource configuration.")
    parser.add_argument("--upload-id", type=UUID, help="Upload to process in this child process.")
    parser.add_argument("--lease-id", type=UUID, help="Worker lease that owns the upload.")
    parser.add_argument("--scratch", type=Path, help="Parent-owned directory for temporary files.")
    args = parser.parse_args()
    if (args.upload_id is None) != (args.lease_id is None):
        parser.error("--upload-id and --lease-id must be supplied together")
    if args.config is not None:
        local_config.configure(args.config)
    configure_logging()
    asyncio.run(_main(args.upload_id, args.lease_id, args.scratch, args.config))


if __name__ == "__main__":
    main()
