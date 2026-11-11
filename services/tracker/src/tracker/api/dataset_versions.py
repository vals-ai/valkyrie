import asyncio
from collections.abc import Generator
from contextlib import contextmanager
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Request, Response, Query
from fastapi.responses import FileResponse
from sqlmodel import Session

from tracker import config
from tracker.api.benchmark_definitions import BenchmarkDefinitionDependency
from tracker.api.datasets import ARCHIVED, DatasetRepositoryDependency
from tracker.api.dependencies import AgentLibraryRuntimeDependency
from tracker.api.download import local_file_response, resolve_download_url
from tracker.api.storage_errors import storage_errors
from tracker.auth import RequestIdentity, get_current_starter
from tracker.database.models import Dataset, DatasetVersion
from tracker.database.session import get_session
from tracker.services.datasets.digest import DIGEST_PATTERN
from tracker.services.datasets.version_ref import LATEST, parse_version_ref, validate_tag
from tracker.exceptions import AmbiguousDigestError, ManifestMismatchError, ResourceArchivedError, TagExistsError
from tracker.services.datasets.publish import (
    InvalidUploadError,
    NotStoredError,
    RejectedUploadError,
    UploadTooLargeError,
    publish_version,
    store_metric_script,
    store_task_archive,
    stored_sizes,
)
from tracker.repositories.dataset_version import DatasetVersionRepository, DatasetVersionRepositoryDependency
from tracker.runtime.artifacts import TASK_ARCHIVE_SUFFIX, dataset_blob_key
from tracker.types import (
    BlobDigestsRequest,
    BlobResponse,
    DatasetTaskDownloadResponse,
    DatasetTaskResponse,
    DatasetTasksPage,
    DatasetVersionDetailResponse,
    DatasetVersionResponse,
    DatasetVersionsResponse,
    ManifestInfo,
    MissingBlobsResponse,
    ResourceName,
    TagRequest,
    TaskId,
    VersionCreateRequest,
)

router = APIRouter(prefix="/benchmarks/{benchmark}/datasets/{name}")

ZIP_MEDIA_TYPE = "application/zip"
PRESIGNED_URL_EXPIRES_SECONDS = 300
NOT_FOUND: dict[int | str, dict[str, Any]] = {
    404: {"description": "The benchmark, the dataset, the version or the task does not exist"}
}
STORAGE_ERRORS: dict[int | str, dict[str, Any]] = {
    403: {"description": "Storage permission denied"},
    502: {"description": "Storage operation failed"},
}
PathDigest = Annotated[str, Path(pattern=f"^{DIGEST_PATTERN}$")]


def get_dataset(
    benchmark: BenchmarkDefinitionDependency, name: ResourceName, datasets: DatasetRepositoryDependency
) -> Dataset:
    dataset = datasets.get(benchmark.id, name)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{name}' not found")
    return dataset


def get_dataset_id(dataset: Annotated[Dataset, Depends(get_dataset)], session: Session = Depends(get_session)) -> UUID:
    dataset_id = dataset.id
    session.close()
    return dataset_id


def get_active_dataset_id(
    dataset: Annotated[Dataset, Depends(get_dataset)], session: Session = Depends(get_session)
) -> UUID:
    if dataset.archived_at is not None:
        raise HTTPException(status_code=409, detail=str(ResourceArchivedError("Dataset", dataset.name)))
    return get_dataset_id(dataset, session)


DatasetDependency = Annotated[Dataset, Depends(get_dataset)]
DatasetIdDependency = Annotated[UUID, Depends(get_dataset_id)]
ActiveDatasetIdDependency = Annotated[UUID, Depends(get_active_dataset_id)]


def _resolve(versions: DatasetVersionRepository, dataset: Dataset, text: str) -> DatasetVersion:
    try:
        ref = parse_version_ref(text)
        version = versions.get(dataset.id, ref)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except AmbiguousDigestError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    if version is None:
        raise HTTPException(status_code=404, detail=f"Version '{text}' of dataset '{dataset.name}' not found")
    return version


def _version_response(benchmark: str, dataset: str, version: DatasetVersion, tags: list[str]) -> DatasetVersionResponse:
    return DatasetVersionResponse(
        benchmark=benchmark,
        dataset=dataset,
        revision=version.revision,
        digest=version.digest,
        task_count=version.task_count,
        tags=tags,
        created_by_email=version.created_by_email,
        created_at=version.created_at,
    )


def _detail_response(
    benchmark: str, dataset: str, versions: DatasetVersionRepository, version: DatasetVersion
) -> DatasetVersionDetailResponse:
    summary = _version_response(benchmark, dataset, version, versions.tags_of([version.id])[version.id])
    manifest = None if version.manifest is None else ManifestInfo.model_validate(version.manifest)
    return DatasetVersionDetailResponse(**summary.model_dump(), metric_digest=version.metric_digest, manifest=manifest)


@contextmanager
def upload_errors() -> Generator[None]:
    try:
        yield
    except UploadTooLargeError as error:
        raise HTTPException(status_code=413, detail=str(error)) from error
    except InvalidUploadError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except (RejectedUploadError, ManifestMismatchError) as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except NotStoredError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post(
    "/blobs/missing",
    response_model=MissingBlobsResponse,
    description="Return the digests among the given ones whose task archive the dataset does not store yet.",
    responses={**NOT_FOUND, **STORAGE_ERRORS},
)
async def missing_blobs(
    dataset_id: DatasetIdDependency, request: BlobDigestsRequest, runtime: AgentLibraryRuntimeDependency
) -> MissingBlobsResponse:
    wanted = list(dict.fromkeys(request.digests))
    with storage_errors("Blob"):
        stored = await stored_sizes(runtime.objects, dataset_id, wanted)
    return MissingBlobsResponse(missing=[digest for digest in wanted if digest not in stored])


@router.put(
    "/blobs/{digest}",
    response_model=BlobResponse,
    description="Store a task's .zip archive under its digest. The Tracker hashes the archive's files and refuses a "
    "mismatch.\n\nIt then checks the task and refuses one that can't run. The response lists the task's warnings.",
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {ZIP_MEDIA_TYPE: {"schema": {"type": "string", "format": "binary"}}},
        }
    },
    responses={
        201: {"description": "The archive is new"},
        400: {"description": "The body is not a safe zip archive"},
        413: {"description": "The archive exceeds DATASET_TASK_UPLOAD_MAX_BYTES"},
        415: {"description": "The content type is not application/zip"},
        422: {"description": "The archive's files do not hash to the digest in the path, or the task is not valid"},
        **ARCHIVED,
        **NOT_FOUND,
        **STORAGE_ERRORS,
    },
)
async def put_blob(
    dataset_id: ActiveDatasetIdDependency,
    digest: PathDigest,
    request: Request,
    response: Response,
    runtime: AgentLibraryRuntimeDependency,
    _identity: RequestIdentity = Depends(get_current_starter),
) -> BlobResponse:
    if request.headers.get("content-type", "").split(";", 1)[0].lower() != ZIP_MEDIA_TYPE:
        raise HTTPException(status_code=415, detail=f"Expected {ZIP_MEDIA_TYPE}")
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > config.DATASET_TASK_UPLOAD_MAX_BYTES:
        raise HTTPException(status_code=413, detail="Task upload exceeds DATASET_TASK_UPLOAD_MAX_BYTES")

    with upload_errors():
        stored = await store_task_archive(runtime.objects, dataset_id, digest, request.stream(), storage_errors)
    response.status_code = 200 if stored.existed else 201
    return BlobResponse(digest=digest, size=stored.size, warnings=stored.warnings)


def require_custom_scoring() -> None:
    if not config.METRIC_RUNNER_FUNCTION:
        raise HTTPException(status_code=403, detail="Custom scoring isn't available in this Valkyrie deployment")


@router.put(
    "/files/{digest}",
    response_model=BlobResponse,
    description="Store a dataset's metric script under the sha256 digest of its bytes, after checking that it is "
    "valid Python.",
    responses={
        201: {"description": "The file is new"},
        400: {"description": "The script is not valid Python or has an invalid inline header"},
        403: {"description": "Custom scoring isn't available in this Valkyrie deployment"},
        413: {"description": "The script is larger than 256 KB"},
        422: {"description": "The bytes do not hash to the digest in the path"},
        **ARCHIVED,
        **NOT_FOUND,
        **STORAGE_ERRORS,
    },
)
async def put_file(
    dataset_id: ActiveDatasetIdDependency,
    digest: PathDigest,
    request: Request,
    response: Response,
    runtime: AgentLibraryRuntimeDependency,
    _identity: RequestIdentity = Depends(get_current_starter),
) -> BlobResponse:
    require_custom_scoring()
    with upload_errors():
        stored = await store_metric_script(runtime.objects, dataset_id, digest, request.stream(), storage_errors)
    response.status_code = 200 if stored.existed else 201
    return BlobResponse(digest=digest, size=stored.size)


@router.post(
    "/versions",
    response_model=DatasetVersionDetailResponse,
    status_code=201,
    description="Publish the version holding exactly these tasks. A version with the same tasks is returned as it is "
    "(200) instead of a new revision. `latest` and the given tags point at the version.\n\nA `manifest` records the "
    "dataset.toml the tasks came from, and the tasks must be exactly the ones it lists with the digests it gives.",
    responses={
        200: {"description": "The dataset already has a version with exactly these tasks"},
        409: {"description": "The dataset is archived, a task's archive is not stored yet, or a tag points elsewhere"},
        422: {"description": "The tasks are not exactly the ones the manifest lists"},
        **NOT_FOUND,
        **STORAGE_ERRORS,
    },
)
async def create_version(
    benchmark: BenchmarkDefinitionDependency,
    dataset_id: ActiveDatasetIdDependency,
    name: ResourceName,
    request: VersionCreateRequest,
    response: Response,
    runtime: AgentLibraryRuntimeDependency,
    versions: DatasetVersionRepositoryDependency,
    identity: RequestIdentity = Depends(get_current_starter),
) -> DatasetVersionDetailResponse:
    if request.metric_digest is not None:
        require_custom_scoring()
    try:
        with upload_errors():
            version, created = await publish_version(
                runtime.objects, versions, dataset_id, request, identity.email, storage_errors
            )
    except TagExistsError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    response.status_code = 201 if created else 200
    return await asyncio.to_thread(_detail_response, benchmark.name, name, versions, version)


@router.get(
    "/versions",
    response_model=DatasetVersionsResponse,
    description="List the dataset's versions, newest first, with the tags that point at each.",
    responses=NOT_FOUND,
)
def list_versions(
    benchmark: BenchmarkDefinitionDependency, dataset: DatasetDependency, versions: DatasetVersionRepositoryDependency
) -> DatasetVersionsResponse:
    rows = versions.list_all(dataset.id)
    tags = versions.tags_of([row.id for row in rows])
    return DatasetVersionsResponse(
        versions=[_version_response(benchmark.name, dataset.name, row, tags[row.id]) for row in rows]
    )


@router.get(
    "/versions/{ref}",
    response_model=DatasetVersionDetailResponse,
    description="Return one version, whose tasks `/versions/{ref}/tasks` lists. `ref` is a revision number, a tag, or a digest of at least 8 hex "
    "characters after `sha256:`.",
    responses={**NOT_FOUND, 409: {"description": "The digest prefix matches more than one version"}},
)
def get_version(
    benchmark: BenchmarkDefinitionDependency,
    dataset: DatasetDependency,
    ref: str,
    versions: DatasetVersionRepositoryDependency,
) -> DatasetVersionDetailResponse:
    return _detail_response(benchmark.name, dataset.name, versions, _resolve(versions, dataset, ref))


@router.get(
    "/versions/{ref}/tasks",
    response_model=DatasetTasksPage,
    description="List one page of a version's tasks in task ID order. Pass `next_cursor` as `cursor` to read the next "
    "page.",
    responses={**NOT_FOUND, 409: {"description": "The digest prefix matches more than one version"}},
)
def list_version_tasks(
    dataset: DatasetDependency,
    ref: str,
    versions: DatasetVersionRepositoryDependency,
    cursor: str | None = None,
    limit: int = Query(default=1000, ge=1, le=1000),
) -> DatasetTasksPage:
    rows = versions.tasks_page(_resolve(versions, dataset, ref).id, cursor, limit + 1)
    tasks = [
        DatasetTaskResponse(task_id=row.task_id, digest=row.digest, size=row.size_bytes, source_name=row.source_name)
        for row in rows[:limit]
    ]
    return DatasetTasksPage(tasks=tasks, next_cursor=tasks[-1].task_id if len(rows) > limit else None)


@router.get(
    "/versions/{ref}/tasks/{task_id}",
    response_model=DatasetTaskDownloadResponse,
    description="Return a task's digest and size in a version, and a temporary URL to download its archive.",
    responses={**NOT_FOUND, **STORAGE_ERRORS},
)
async def get_version_task(
    dataset: DatasetDependency,
    ref: str,
    task_id: TaskId,
    runtime: AgentLibraryRuntimeDependency,
    request: Request,
    versions: DatasetVersionRepositoryDependency,
    download: bool = False,
) -> DatasetTaskDownloadResponse | FileResponse:
    version = _resolve(versions, dataset, ref)
    task = versions.task(version.id, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' is not in version '{ref}'")
    key = dataset_blob_key(str(dataset.id), task.digest)
    with storage_errors("Task"):
        if download:
            file_response = local_file_response(
                runtime.objects, key, filename=f"{task_id}{TASK_ARCHIVE_SUFFIX}", media_type=ZIP_MEDIA_TYPE
            )
            if file_response is not None:
                return file_response
        url, expires_in = await resolve_download_url(
            runtime.objects,
            key,
            request=request,
            route_name="get_version_task",
            route_params=request.path_params,
            expires_in=PRESIGNED_URL_EXPIRES_SECONDS,
        )
    return DatasetTaskDownloadResponse(
        task_id=task_id, digest=task.digest, size=task.size_bytes, download_url=url, expires_in=expires_in
    )


@router.put(
    "/tags/{tag}",
    response_model=DatasetVersionResponse,
    description=(
        "Point a tag at the version `ref` names. Returns 409 when the tag points at another version, "
        "unless `force` is set or the tag is `latest`."
    ),
    responses={
        400: {"description": "The tag or the reference is not valid"},
        409: {"description": "The dataset is archived, or the tag points at another version and `force` is not set"},
        **NOT_FOUND,
    },
)
def put_tag(
    benchmark: BenchmarkDefinitionDependency,
    dataset: DatasetDependency,
    tag: str,
    request: TagRequest,
    versions: DatasetVersionRepositoryDependency,
    _identity: RequestIdentity = Depends(get_current_starter),
) -> DatasetVersionResponse:
    if dataset.archived_at is not None:
        raise HTTPException(status_code=409, detail=str(ResourceArchivedError("Dataset", dataset.name)))
    try:
        validate_tag(tag)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    version = _resolve(versions, dataset, request.ref)
    try:
        versions.point_tag(dataset.id, tag, version.id, request.force)
    except TagExistsError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return _version_response(benchmark.name, dataset.name, version, versions.tags_of([version.id])[version.id])


@router.delete(
    "/tags/{tag}",
    response_model=DatasetVersionResponse,
    description="Remove a tag and return the version it pointed at. `latest` can't be removed.",
    responses={409: {"description": "The dataset is archived, or the tag is `latest`"}, **NOT_FOUND},
)
def delete_tag(
    benchmark: BenchmarkDefinitionDependency,
    dataset: DatasetDependency,
    tag: str,
    versions: DatasetVersionRepositoryDependency,
    _identity: RequestIdentity = Depends(get_current_starter),
) -> DatasetVersionResponse:
    if dataset.archived_at is not None:
        raise HTTPException(status_code=409, detail=str(ResourceArchivedError("Dataset", dataset.name)))
    if tag == LATEST:
        raise HTTPException(status_code=409, detail="The latest tag can't be removed")
    version = versions.remove_tag(dataset.id, tag)
    if version is None:
        raise HTTPException(status_code=404, detail=f"Tag '{tag}' of dataset '{dataset.name}' not found")
    return _version_response(benchmark.name, dataset.name, version, versions.tags_of([version.id])[version.id])
