from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlmodel import Session

from tracker import config
from tracker.api.dataset_versions import DatasetIdDependency, require_custom_scoring
from tracker.api.dependencies import AgentLibraryRuntimeDependency
from tracker.api.storage_errors import storage_errors
from tracker.auth import RequestIdentity, get_current_identity, get_current_starter
from tracker.database.models import DatasetUpload
from tracker.database.session import database_call, get_session
from tracker.domain import DatasetUploadStatus
from tracker.exceptions import DatasetUploadConflictError, ResourceArchivedError
from tracker.repositories.dataset_upload import DatasetUploadRepositoryDependency
from tracker.runtime.artifacts import dataset_upload_key
from tracker.types import (
    DatasetTaskResponse,
    DatasetTasksPage,
    DatasetUploadCreateRequest,
    DatasetUploadFormResponse,
    DatasetUploadPublishRequest,
    DatasetUploadResponse,
    DatasetUploadsResponse,
    ErrorMessage,
    TaskId,
    VersionCreateRequest,
)

router = APIRouter(
    prefix="/benchmarks/{benchmark}/datasets/{name}/uploads",
    responses={
        403: {
            "model": ErrorMessage,
            "description": "The caller needs an identifiable user or access key with permission for this operation.",
        },
        404: {
            "model": ErrorMessage,
            "description": "The benchmark or dataset does not exist, or the caller has no upload with this ID.",
        },
    },
)

DATASET_NOT_FOUND: dict[int | str, dict[str, Any]] = {
    404: {"model": ErrorMessage, "description": "The benchmark or dataset does not exist."},
}
UPLOAD_STORAGE_ERRORS: dict[int | str, dict[str, Any]] = {
    403: {"model": ErrorMessage, "description": "Tracker or storage denied access to this upload."},
    502: {"model": ErrorMessage, "description": "The upload storage operation failed."},
}


def _owner(response: Response, identity: RequestIdentity = Depends(get_current_identity)) -> str:
    response.headers["Cache-Control"] = "private, no-store"
    if identity.user_id:
        return f"user:{identity.user_id}"
    if identity.access_key_id:
        return f"key:{identity.access_key_id}"
    if not config.AUTH_REQUIRED:
        return "local"
    raise HTTPException(status_code=403, detail="Archive uploads require an identifiable user or access key")


OwnerDependency = Annotated[str, Depends(_owner)]


def _response(upload: DatasetUpload) -> DatasetUploadResponse:
    response = DatasetUploadResponse.model_validate(upload, from_attributes=True)
    if upload.status == DatasetUploadStatus.SUCCEEDED:
        response.published_version_digest = upload.review_digest
    return response


def get_upload(
    dataset_id: DatasetIdDependency,
    upload_id: UUID,
    owner: OwnerDependency,
    repository: DatasetUploadRepositoryDependency,
) -> DatasetUpload:
    upload = repository.get_upload(dataset_id, upload_id, owner)
    if upload is None:
        raise HTTPException(status_code=404, detail="Upload not found")
    return upload


UploadDependency = Annotated[DatasetUpload, Depends(get_upload)]


def upload_snapshot(upload: UploadDependency, session: Session = Depends(get_session)) -> DatasetUploadResponse:
    response = _response(upload)
    session.close()
    return response


UploadSnapshotDependency = Annotated[DatasetUploadResponse, Depends(upload_snapshot)]


@contextmanager
def upload_errors() -> Generator[None]:
    try:
        yield
    except (DatasetUploadConflictError, ResourceArchivedError) as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.put(
    "/{upload_id}",
    response_model=DatasetUploadResponse,
    description="Reserve an upload ID for one archive checksum and size. Repeating the same request returns the saved upload.",
    responses={
        **DATASET_NOT_FOUND,
        409: {
            "model": ErrorMessage,
            "description": "The benchmark or dataset is archived, the upload ID is already reserved for another request, or the dataset has three active uploads.",
        },
        413: {"model": ErrorMessage, "description": "The archive exceeds the configured upload size limit."},
    },
)
def create_upload(
    dataset_id: DatasetIdDependency,
    upload_id: UUID,
    request: DatasetUploadCreateRequest,
    owner: OwnerDependency,
    repository: DatasetUploadRepositoryDependency,
    identity: RequestIdentity = Depends(get_current_starter),
) -> DatasetUploadResponse:
    if request.archive_size > config.DATASET_ARCHIVE_UPLOAD_MAX_BYTES:
        raise HTTPException(status_code=413, detail="The archive exceeds DATASET_ARCHIVE_UPLOAD_MAX_BYTES")
    with upload_errors():
        return _response(
            repository.create_upload(
                dataset_id, upload_id, owner, request.archive_sha256, request.archive_size, identity.email
            )
        )


@router.get(
    "",
    response_model=DatasetUploadsResponse,
    description="List the caller's recent uploads for this dataset.",
    responses=DATASET_NOT_FOUND,
)
def list_uploads(
    dataset_id: DatasetIdDependency,
    owner: OwnerDependency,
    repository: DatasetUploadRepositoryDependency,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> DatasetUploadsResponse:
    return DatasetUploadsResponse(uploads=[_response(row) for row in repository.list_uploads(dataset_id, owner, limit)])


@router.get(
    "/{upload_id}",
    response_model=DatasetUploadResponse,
    description="Return the caller's upload status, review summary, and published version digest.",
)
def read_upload(upload: UploadDependency) -> DatasetUploadResponse:
    return _response(upload)


@router.post(
    "/{upload_id}/form",
    response_model=DatasetUploadFormResponse,
    description="Issue a temporary form for the reserved archive. The storage destination enforces its exact size and checksum.",
    responses={
        **UPLOAD_STORAGE_ERRORS,
        400: {"model": ErrorMessage, "description": "Storage cannot issue a form for the reserved archive size."},
        404: {
            "model": ErrorMessage,
            "description": "The benchmark, dataset, caller's upload, or storage destination does not exist.",
        },
        409: {"model": ErrorMessage, "description": "The upload expired or no longer accepts an archive."},
        501: {"model": ErrorMessage, "description": "This deployment does not support direct archive uploads."},
    },
)
async def upload_form(
    dataset_id: DatasetIdDependency, upload: UploadSnapshotDependency, runtime: AgentLibraryRuntimeDependency
) -> DatasetUploadFormResponse:
    remaining = int((upload.expires_at - datetime.now(UTC)).total_seconds())
    if upload.status != DatasetUploadStatus.UPLOADING or remaining < 1:
        raise HTTPException(status_code=409, detail="This upload no longer accepts an archive")
    with storage_errors("Upload"):
        form = await runtime.objects.temporary_upload_form(
            dataset_upload_key(str(dataset_id), str(upload.id)),
            size=upload.archive_size,
            sha256=upload.archive_sha256,
            expires_in=min(remaining, 600),
        )
    if form is None:
        raise HTTPException(status_code=501, detail="This deployment does not support direct archive uploads")
    return DatasetUploadFormResponse(url=form.url, fields=form.fields, expires_in=form.expires_in)


@router.post(
    "/{upload_id}/complete",
    response_model=DatasetUploadResponse,
    description="Queue an uploaded archive for validation.",
    responses={
        **UPLOAD_STORAGE_ERRORS,
        400: {"model": ErrorMessage, "description": "Storage rejected the archive path."},
        404: {
            "model": ErrorMessage,
            "description": "The benchmark, dataset, caller's upload, or stored archive does not exist.",
        },
        409: {
            "model": ErrorMessage,
            "description": "The upload expired or ended, or the stored archive size differs from the reservation.",
        },
    },
)
async def complete_upload(
    dataset_id: DatasetIdDependency,
    upload: UploadSnapshotDependency,
    owner: OwnerDependency,
    repository: DatasetUploadRepositoryDependency,
    runtime: AgentLibraryRuntimeDependency,
) -> DatasetUploadResponse:
    if upload.status == DatasetUploadStatus.UPLOADING:
        with storage_errors("Upload archive"):
            stored = await runtime.objects.stat(dataset_upload_key(str(dataset_id), str(upload.id)))
        if stored.size != upload.archive_size:
            raise HTTPException(status_code=409, detail="The stored archive does not match the reserved size")

    def queue() -> DatasetUploadResponse:
        with upload_errors():
            return _response(repository.queue_upload(dataset_id, upload.id, owner))

    return await database_call(queue)


@router.get(
    "/{upload_id}/tasks",
    response_model=DatasetTasksPage,
    description="List the reviewed tasks in task ID order. Publication uses this exact task map.",
    responses={409: {"model": ErrorMessage, "description": "The upload has no validated task map."}},
)
def reviewed_tasks(
    upload: UploadDependency,
    cursor: TaskId | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
) -> DatasetTasksPage:
    if upload.review is None:
        raise HTTPException(status_code=409, detail="This upload has no validated task map")
    request = VersionCreateRequest.model_validate(upload.review["request"])
    sizes = upload.review["sizes"]
    selected = sorted(task_id for task_id in request.tasks if cursor is None or task_id > cursor)[: limit + 1]
    return DatasetTasksPage(
        tasks=[
            DatasetTaskResponse(
                task_id=task_id,
                digest=request.tasks[task_id],
                size=sizes[request.tasks[task_id]],
                source_name=None if request.manifest is None else request.manifest.tasks[task_id].name,
            )
            for task_id in selected[:limit]
        ],
        next_cursor=selected[limit - 1] if len(selected) > limit else None,
    )


@router.post(
    "/{upload_id}/publish",
    response_model=DatasetUploadResponse,
    description="Publish the confirmed review digest and move `latest` to that version. Repeated confirmation returns the saved status.",
    responses={
        403: {
            "model": ErrorMessage,
            "description": "The caller lacks permission to publish or the deployment does not support the upload's custom scoring.",
        },
        409: {
            "model": ErrorMessage,
            "description": "The review digest does not match or the upload is not ready for publication.",
        },
    },
)
def publish_upload(
    dataset_id: DatasetIdDependency,
    upload: UploadDependency,
    request: DatasetUploadPublishRequest,
    owner: OwnerDependency,
    repository: DatasetUploadRepositoryDependency,
) -> DatasetUploadResponse:
    if upload.metric_digest is not None:
        require_custom_scoring()
    with upload_errors():
        return _response(repository.request_publication(dataset_id, upload.id, owner, request.review_digest))


@router.post(
    "/{upload_id}/cancel",
    response_model=DatasetUploadResponse,
    description="Cancel the caller's upload before publication starts. Repeated cancellation returns the saved status.",
    responses={409: {"model": ErrorMessage, "description": "Publication has already started."}},
)
def cancel_upload(
    dataset_id: DatasetIdDependency,
    upload: UploadDependency,
    owner: OwnerDependency,
    repository: DatasetUploadRepositoryDependency,
) -> DatasetUploadResponse:
    with upload_errors():
        return _response(repository.cancel_upload(dataset_id, upload.id, owner))
