from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse
from sqlmodel import Session

from tracker import config
from tracker.api.benchmark_definitions import BenchmarkDefinitionDependency
from tracker.repositories.dataset import DatasetRepositoryDependency
from tracker.api.dependencies import AgentLibraryRuntimeDependency
from tracker.api.download import local_file_response, resolve_download_url
from tracker.api.storage_errors import storage_errors
from tracker.database.models import Dataset
from tracker.database.session import get_session
from tracker.exceptions import ResourceArchivedError
from tracker.runtime.artifacts import (
    TASK_ARCHIVE_SUFFIX,
    dataset_task_id,
    dataset_task_key,
    dataset_tasks_prefix,
)
from tracker.runtime.storage import StoredObject
from tracker.types import (
    DatasetTaskDownloadResponse,
    DatasetTaskResponse,
    DatasetTasksResponse,
    ResourceName,
    TaskId,
)

router = APIRouter(prefix="/benchmarks/{benchmark}/datasets/{name}/tasks")

ZIP_MEDIA_TYPE = "application/zip"
ZIP_MAGIC = b"PK\x03\x04"
PRESIGNED_URL_EXPIRES_SECONDS = 300
NOT_FOUND = {404: {"description": "The benchmark, the dataset or the task does not exist"}}
ARCHIVED = {409: {"description": "The dataset is archived; restore it first"}}
STORAGE_ERRORS = {403: {"description": "Storage permission denied"}, 502: {"description": "Storage operation failed"}}


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


DatasetIdDependency = Annotated[UUID, Depends(get_dataset_id)]
ActiveDatasetIdDependency = Annotated[UUID, Depends(get_active_dataset_id)]


def _task(stored: StoredObject, task_id: str) -> DatasetTaskResponse:
    return DatasetTaskResponse(task_id=task_id, size=stored.size, last_modified=stored.last_modified)


async def _zip_chunks(request: Request) -> AsyncIterator[bytes]:
    head = bytearray()
    uploaded = 0
    async for chunk in request.stream():
        if len(head) < len(ZIP_MAGIC):
            head.extend(chunk[: len(ZIP_MAGIC) - len(head)])
            if len(head) == len(ZIP_MAGIC) and head != ZIP_MAGIC:
                raise HTTPException(status_code=400, detail="Expected a zip archive")
        uploaded += len(chunk)
        if uploaded > config.DATASET_TASK_UPLOAD_MAX_BYTES:
            raise HTTPException(status_code=413, detail="Task upload exceeds DATASET_TASK_UPLOAD_MAX_BYTES")
        yield chunk
    if head != ZIP_MAGIC:
        raise HTTPException(status_code=400, detail="Expected a zip archive")


@router.get(
    "",
    response_model=DatasetTasksResponse,
    description="List a dataset's tasks a page at a time, in the order the bucket returns them.",
    responses={**NOT_FOUND, **STORAGE_ERRORS},
)
async def list_dataset_tasks(
    dataset_id: DatasetIdDependency,
    runtime: AgentLibraryRuntimeDependency,
    cursor: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> DatasetTasksResponse:
    with storage_errors("Task"):
        try:
            stored, next_cursor = await runtime.objects.list_objects_page(
                dataset_tasks_prefix(str(dataset_id)), cursor=cursor, limit=limit
            )
        except ValueError as error:
            raise HTTPException(status_code=400, detail="Invalid cursor") from error
    return DatasetTasksResponse(
        tasks=[
            _task(item, task_id)
            for item in stored
            if (task_id := dataset_task_id(str(dataset_id), item.key)) is not None
        ],
        next_cursor=next_cursor,
    )


@router.put(
    "/{task_id}",
    response_model=DatasetTaskResponse,
    description="Store a task's .zip archive, replacing the task if it exists. The archive streams to storage.",
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {ZIP_MEDIA_TYPE: {"schema": {"type": "string", "format": "binary"}}},
        }
    },
    responses={
        201: {"description": "The task is new"},
        400: {"description": "The body is not a zip archive"},
        413: {"description": "The archive exceeds DATASET_TASK_UPLOAD_MAX_BYTES"},
        415: {"description": "The content type is not application/zip"},
        **ARCHIVED,
        **NOT_FOUND,
        **STORAGE_ERRORS,
    },
)
async def put_dataset_task(
    dataset_id: ActiveDatasetIdDependency,
    task_id: TaskId,
    request: Request,
    response: Response,
    runtime: AgentLibraryRuntimeDependency,
) -> DatasetTaskResponse:
    key = dataset_task_key(str(dataset_id), task_id)
    if request.headers.get("content-type", "").split(";", 1)[0].lower() != ZIP_MEDIA_TYPE:
        raise HTTPException(status_code=415, detail=f"Expected {ZIP_MEDIA_TYPE}")
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > config.DATASET_TASK_UPLOAD_MAX_BYTES:
        raise HTTPException(status_code=413, detail="Task upload exceeds DATASET_TASK_UPLOAD_MAX_BYTES")

    with storage_errors("Task"):
        existed = await runtime.objects.exists(key)
        await runtime.objects.put_stream(key, _zip_chunks(request))
        stored = await runtime.objects.stat(key)

    response.status_code = 200 if existed else 201
    return _task(stored, task_id)


@router.get(
    "/{task_id}",
    response_model=DatasetTaskDownloadResponse,
    description="Return a task's size and a temporary URL to download its archive.",
    responses={**NOT_FOUND, **STORAGE_ERRORS},
)
async def get_dataset_task(
    dataset_id: DatasetIdDependency,
    task_id: TaskId,
    runtime: AgentLibraryRuntimeDependency,
    request: Request,
    download: bool = False,
) -> DatasetTaskDownloadResponse | FileResponse:
    key = dataset_task_key(str(dataset_id), task_id)
    with storage_errors("Task"):
        stored = await runtime.objects.stat(key)
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
            route_name="get_dataset_task",
            route_params=request.path_params,
            expires_in=PRESIGNED_URL_EXPIRES_SECONDS,
        )
    return DatasetTaskDownloadResponse(
        task_id=task_id, size=stored.size, last_modified=stored.last_modified, download_url=url, expires_in=expires_in
    )


@router.delete(
    "/{task_id}",
    response_model=DatasetTaskResponse,
    description="Delete a task's archive from the dataset's storage and return what was deleted.",
    responses={**ARCHIVED, **NOT_FOUND, **STORAGE_ERRORS},
)
async def delete_dataset_task(
    dataset_id: ActiveDatasetIdDependency, task_id: TaskId, runtime: AgentLibraryRuntimeDependency
) -> DatasetTaskResponse:
    key = dataset_task_key(str(dataset_id), task_id)
    with storage_errors("Task"):
        stored = await runtime.objects.stat(key)
        await runtime.objects.delete(key)
    return _task(stored, task_id)
