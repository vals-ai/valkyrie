import re
from typing import Annotated
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy.exc import IntegrityError
from fastapi import Depends
from sqlmodel import Session, col, func, select

from tracker.database.session import get_session
from tracker import config
from tracker.database.models import BenchmarkDefinition, Dataset, DatasetUpload, DatasetVersion, Org
from tracker.domain import DatasetUploadStatus
from tracker.repositories.dataset_version import DatasetVersionRepository
from tracker.services.datasets.digest import version_digest
from tracker.services.datasets.manifest import check_manifest
from tracker.types import OrgContext, VersionCreateRequest
from tracker.exceptions import DatasetUploadConflictError, ResourceArchivedError


_UPLOAD_TTL = timedelta(hours=24)
_UPLOAD_LEASE = timedelta(minutes=2)
_UPLOAD_DEADLINE = timedelta(minutes=30)
_UPLOAD_MAX_ATTEMPTS = 3
_UPLOAD_MAX_ACTIVE = 3
_UPLOAD_TERMINAL = (
    DatasetUploadStatus.SUCCEEDED,
    DatasetUploadStatus.CANCELLED,
    DatasetUploadStatus.FAILED,
    DatasetUploadStatus.EXPIRED,
)
_UPLOAD_PROCESSING = (DatasetUploadStatus.VALIDATING, DatasetUploadStatus.PUBLISHING)
_UPLOAD_QUEUED = (DatasetUploadStatus.QUEUED, DatasetUploadStatus.PUBLISH_QUEUED)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _release(upload: DatasetUpload, status: DatasetUploadStatus, now: datetime) -> None:
    upload.status = status
    upload.updated_at = now
    upload.lease_id = None
    upload.lease_expires_at = None


def _review_digest(request: VersionCreateRequest) -> str:
    return version_digest(request.tasks, {} if request.metric_digest is None else {"metric.py": request.metric_digest})


def _now() -> datetime:
    return datetime.now(UTC)


class DatasetUploadRepository:
    def __init__(self, session: Session = Depends(get_session)) -> None:
        self._session = session

    def create_upload(
        self,
        dataset_id: UUID,
        upload_id: UUID,
        owner: str,
        archive_sha256: str,
        archive_size: int,
        created_by_email: str | None,
    ) -> DatasetUpload:
        if not owner or re.fullmatch(r"[0-9a-f]{64}", archive_sha256) is None:
            raise ValueError("An upload requires an owner and a SHA-256 checksum")
        if not 1 <= archive_size <= config.DATASET_ARCHIVE_UPLOAD_MAX_BYTES:
            raise ValueError("The archive exceeds the upload size limits")
        self._active_dataset(dataset_id)
        existing = self._session.get(DatasetUpload, upload_id)
        if existing is not None:
            if (existing.dataset_id, existing.owner, existing.archive_sha256, existing.archive_size) != (
                dataset_id,
                owner,
                archive_sha256,
                archive_size,
            ):
                raise DatasetUploadConflictError("This upload ID already identifies another archive")
            self._session.commit()
            return existing
        now = _now()
        active = self._session.exec(
            select(func.count())
            .select_from(DatasetUpload)
            .where(
                col(DatasetUpload.dataset_id) == dataset_id,
                col(DatasetUpload.status).not_in(_UPLOAD_TERMINAL),
                col(DatasetUpload.expires_at) > now,
            )
        ).one()
        if active >= _UPLOAD_MAX_ACTIVE:
            raise DatasetUploadConflictError("This dataset already has three active uploads")
        upload = DatasetUpload(
            id=upload_id,
            dataset_id=dataset_id,
            owner=owner,
            archive_sha256=archive_sha256,
            archive_size=archive_size,
            created_by_email=created_by_email,
            created_at=now,
            updated_at=now,
            expires_at=now + _UPLOAD_TTL,
            archive_cleanup_at=now,
        )
        self._session.add(upload)
        try:
            self._session.commit()
        except IntegrityError as error:
            self._session.rollback()
            if self._session.get(DatasetUpload, upload_id) is not None:
                raise DatasetUploadConflictError("This upload ID already identifies another archive") from error
            raise
        return upload

    def get_upload(self, dataset_id: UUID, upload_id: UUID, owner: str) -> DatasetUpload | None:
        return self._session.exec(
            select(DatasetUpload).where(
                col(DatasetUpload.id) == upload_id,
                col(DatasetUpload.dataset_id) == dataset_id,
                col(DatasetUpload.owner) == owner,
            )
        ).first()

    def list_uploads(self, dataset_id: UUID, owner: str, limit: int = 20) -> list[DatasetUpload]:
        return list(
            self._session.exec(
                select(DatasetUpload)
                .where(
                    col(DatasetUpload.dataset_id) == dataset_id,
                    col(DatasetUpload.owner) == owner,
                )
                .order_by(col(DatasetUpload.created_at).desc(), col(DatasetUpload.id))
                .limit(min(max(limit, 1), 100))
            ).all()
        )

    def queue_upload(self, dataset_id: UUID, upload_id: UUID, owner: str) -> DatasetUpload:
        upload = self._owned_upload(dataset_id, upload_id, owner)
        now = _now()
        if upload.status in _UPLOAD_TERMINAL or _utc(upload.expires_at) <= now:
            raise DatasetUploadConflictError("This upload no longer accepts an archive")
        if upload.status == DatasetUploadStatus.UPLOADING:
            upload.status = DatasetUploadStatus.QUEUED
            upload.updated_at = now
        self._session.commit()
        return upload

    def cancel_upload(self, dataset_id: UUID, upload_id: UUID, owner: str) -> DatasetUpload:
        upload = self._owned_upload(dataset_id, upload_id, owner)
        if upload.status in (DatasetUploadStatus.PUBLISHING, DatasetUploadStatus.SUCCEEDED):
            raise DatasetUploadConflictError("Publication already started")
        if upload.status not in _UPLOAD_TERMINAL:
            upload.error = "Upload cancelled"
            _release(upload, DatasetUploadStatus.CANCELLED, _now())
        self._session.commit()
        return upload

    def request_publication(self, dataset_id: UUID, upload_id: UUID, owner: str, digest: str) -> DatasetUpload:
        upload = self._owned_upload(dataset_id, upload_id, owner)
        if upload.review_digest != digest:
            raise DatasetUploadConflictError("The requested version does not match this upload's review")
        if upload.status in (
            DatasetUploadStatus.PUBLISH_QUEUED,
            DatasetUploadStatus.PUBLISHING,
            DatasetUploadStatus.SUCCEEDED,
        ):
            self._session.commit()
            return upload
        now = _now()
        if upload.status != DatasetUploadStatus.READY or _utc(upload.expires_at) <= now:
            raise DatasetUploadConflictError("This upload is not ready for publication")
        upload.status = DatasetUploadStatus.PUBLISH_QUEUED
        upload.updated_at = now
        upload.expires_at = now + _UPLOAD_TTL
        upload.attempts = 0
        upload.processing_deadline = None
        upload.error = None
        self._session.commit()
        return upload

    def claim_upload(self) -> DatasetUpload | None:
        now = _now()
        upload = self._session.exec(
            select(DatasetUpload)
            .where(
                col(DatasetUpload.expires_at) > now,
                col(DatasetUpload.attempts) < _UPLOAD_MAX_ATTEMPTS,
                col(DatasetUpload.processing_deadline).is_(None) | (col(DatasetUpload.processing_deadline) > now),
                col(DatasetUpload.status).in_(_UPLOAD_QUEUED)
                | (col(DatasetUpload.status).in_(_UPLOAD_PROCESSING) & (col(DatasetUpload.lease_expires_at) <= now)),
            )
            .order_by(col(DatasetUpload.created_at), col(DatasetUpload.id))
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
            .limit(1)
        ).first()
        if upload is None:
            self._session.commit()
            return None
        upload.status = (
            DatasetUploadStatus.PUBLISHING
            if upload.status in (DatasetUploadStatus.PUBLISH_QUEUED, DatasetUploadStatus.PUBLISHING)
            else DatasetUploadStatus.VALIDATING
        )
        upload.lease_id = uuid4()
        upload.attempts += 1
        upload.updated_at = now
        if upload.processing_deadline is None:
            upload.processing_deadline = now + _UPLOAD_DEADLINE
        upload.lease_expires_at = min(now + _UPLOAD_LEASE, _utc(upload.processing_deadline), _utc(upload.expires_at))
        self._session.commit()
        return upload

    def renew_upload(self, upload_id: UUID, lease_id: UUID) -> bool:
        try:
            upload = self._leased_upload(upload_id, lease_id)
        except DatasetUploadConflictError:
            self._session.rollback()
            return False
        now = _now()
        assert upload.processing_deadline is not None
        upload.lease_expires_at = min(now + _UPLOAD_LEASE, _utc(upload.processing_deadline), _utc(upload.expires_at))
        upload.updated_at = now
        self._session.commit()
        return True

    def claimed_upload(self, upload_id: UUID, lease_id: UUID) -> DatasetUpload:
        upload = self._leased_upload(upload_id, lease_id)
        self._session.commit()
        return upload

    def upload_storage(self, dataset_id: UUID) -> tuple[OrgContext, str]:
        dataset, benchmark, org = self._session.exec(
            select(Dataset, BenchmarkDefinition, Org)
            .join(BenchmarkDefinition, col(BenchmarkDefinition.id) == col(Dataset.benchmark_definition_id))
            .join(Org, col(Org.id) == col(BenchmarkDefinition.org_id))
            .where(col(Dataset.id) == dataset_id)
        ).one()
        return OrgContext(id=org.id, name=org.name, storage_key=org.storage_key), f"{benchmark.name}/{dataset.name}"

    def claim_upload_cleanup(self) -> list[DatasetUpload]:
        now = _now()
        uploads = self._session.exec(
            select(DatasetUpload)
            .where(
                col(DatasetUpload.archive_cleanup_at) <= now,
                col(DatasetUpload.status).not_in(
                    (DatasetUploadStatus.UPLOADING, DatasetUploadStatus.QUEUED, DatasetUploadStatus.VALIDATING)
                ),
            )
            .order_by(col(DatasetUpload.archive_cleanup_at), col(DatasetUpload.id))
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
            .limit(10)
        ).all()
        for upload in uploads:
            upload.archive_cleanup_at = now + timedelta(minutes=1)
        self._session.commit()
        return list(uploads)

    def finish_upload_cleanup(self, upload_id: UUID) -> None:
        upload = self._session.get(DatasetUpload, upload_id)
        if upload is not None:
            upload.archive_cleanup_at = None
            self._session.commit()

    def complete_upload(
        self,
        upload_id: UUID,
        lease_id: UUID,
        request: VersionCreateRequest,
        sizes: Mapping[str, int],
        warnings: Sequence[str] = (),
    ) -> DatasetUpload:
        upload = self._leased_upload(upload_id, lease_id)
        if upload.status != DatasetUploadStatus.VALIDATING:
            raise DatasetUploadConflictError("This upload is not being validated")
        if set(sizes) != set(request.tasks.values()) or any(size < 1 for size in sizes.values()):
            raise ValueError("The reviewed archives do not match the task map")
        if request.manifest is not None:
            check_manifest(request.tasks, request.manifest)
        upload.review = {"request": request.model_dump(mode="json"), "sizes": dict(sizes)}
        upload.review_digest = _review_digest(request)
        upload.task_count = len(request.tasks)
        upload.total_task_bytes = sum(sizes[digest] for digest in request.tasks.values())
        upload.metric_digest = request.metric_digest
        upload.tags = request.tags
        upload.warnings = [message[:2000] for message in warnings[:200]]
        upload.error = None
        upload.processing_deadline = None
        _release(upload, DatasetUploadStatus.READY, _now())
        self._session.commit()
        return upload

    def fail_upload(self, upload_id: UUID, lease_id: UUID, error: str, *, retryable: bool = False) -> None:
        upload = self._leased_upload(upload_id, lease_id)
        if retryable and upload.attempts < _UPLOAD_MAX_ATTEMPTS:
            status = (
                DatasetUploadStatus.PUBLISH_QUEUED
                if upload.status == DatasetUploadStatus.PUBLISHING
                else DatasetUploadStatus.QUEUED
            )
        elif upload.status == DatasetUploadStatus.PUBLISHING:
            status = DatasetUploadStatus.READY
            upload.processing_deadline = None
        else:
            status = DatasetUploadStatus.FAILED
        upload.error = error[:2000]
        _release(upload, status, _now())
        self._session.commit()

    def publication_request(self, upload_id: UUID, lease_id: UUID) -> VersionCreateRequest:
        _, request = self._publishing_upload(upload_id, lease_id)
        return request

    def publish_upload(
        self, upload_id: UUID, lease_id: UUID, request: VersionCreateRequest, sizes: Mapping[str, int]
    ) -> DatasetVersion:
        upload, reviewed = self._publishing_upload(upload_id, lease_id)
        if reviewed != request or _review_digest(request) != upload.review_digest:
            raise DatasetUploadConflictError("The reviewed publication request changed")
        self._active_dataset(upload.dataset_id)
        version, _ = DatasetVersionRepository(self._session).publish(
            upload.dataset_id,
            {task_id: (digest, sizes[digest]) for task_id, digest in request.tasks.items()},
            request.tags,
            upload.created_by_email,
            request.manifest,
            request.metric_digest,
            commit=False,
        )
        now = _now()
        assert upload.processing_deadline is not None
        if now >= min(_utc(upload.processing_deadline), _utc(upload.expires_at)):
            self._session.rollback()
            raise DatasetUploadConflictError("Publication exceeded the upload deadline")
        upload.published_version_id = version.id
        upload.error = None
        _release(upload, DatasetUploadStatus.SUCCEEDED, now)
        self._session.commit()
        return version

    def expire_uploads(self) -> int:
        now = _now()
        uploads = self._session.exec(
            select(DatasetUpload)
            .where(
                col(DatasetUpload.status).not_in(_UPLOAD_TERMINAL),
                (col(DatasetUpload.expires_at) <= now)
                | (col(DatasetUpload.processing_deadline) <= now)
                | (
                    (col(DatasetUpload.attempts) >= _UPLOAD_MAX_ATTEMPTS)
                    & col(DatasetUpload.status).in_(_UPLOAD_PROCESSING)
                    & (col(DatasetUpload.lease_expires_at) <= now)
                ),
            )
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
            .limit(100)
        ).all()
        for upload in uploads:
            expired = _utc(upload.expires_at) <= now
            upload.error = "The upload expired" if expired else "Archive processing exceeded its retry or time limit"
            _release(upload, DatasetUploadStatus.EXPIRED if expired else DatasetUploadStatus.FAILED, now)
        self._session.commit()
        return len(uploads)

    def _owned_upload(self, dataset_id: UUID, upload_id: UUID, owner: str) -> DatasetUpload:
        upload = self._session.exec(
            select(DatasetUpload)
            .where(
                col(DatasetUpload.id) == upload_id,
                col(DatasetUpload.dataset_id) == dataset_id,
                col(DatasetUpload.owner) == owner,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        ).first()
        if upload is None:
            raise DatasetUploadConflictError("Upload not found")
        return upload

    def _publishing_upload(self, upload_id: UUID, lease_id: UUID) -> tuple[DatasetUpload, VersionCreateRequest]:
        upload = self._leased_upload(upload_id, lease_id)
        if upload.status != DatasetUploadStatus.PUBLISHING or upload.review is None:
            raise DatasetUploadConflictError("This upload has no publication request")
        return upload, VersionCreateRequest.model_validate(upload.review["request"])

    def _leased_upload(self, upload_id: UUID, lease_id: UUID) -> DatasetUpload:
        now = _now()
        upload = self._session.exec(
            select(DatasetUpload)
            .where(
                col(DatasetUpload.id) == upload_id,
                col(DatasetUpload.lease_id) == lease_id,
                col(DatasetUpload.status).in_(_UPLOAD_PROCESSING),
                col(DatasetUpload.lease_expires_at) > now,
                col(DatasetUpload.processing_deadline) > now,
                col(DatasetUpload.expires_at) > now,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        ).first()
        if upload is None:
            raise DatasetUploadConflictError("This worker no longer owns the upload")
        return upload

    def _active_dataset(self, dataset_id: UUID) -> Dataset:
        dataset = self._session.get(Dataset, dataset_id)
        if dataset is None:
            raise DatasetUploadConflictError("Dataset not found")
        benchmark = self._session.exec(
            select(BenchmarkDefinition)
            .where(col(BenchmarkDefinition.id) == dataset.benchmark_definition_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        ).one()
        dataset = self._session.exec(
            select(Dataset)
            .where(col(Dataset.id) == dataset_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        ).one()
        if dataset.archived_at is not None:
            raise ResourceArchivedError("Dataset", dataset.name)
        if benchmark.archived_at is not None:
            raise ResourceArchivedError("Benchmark", benchmark.name)
        return dataset


DatasetUploadRepositoryDependency = Annotated[DatasetUploadRepository, Depends(DatasetUploadRepository)]
