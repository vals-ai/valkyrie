from typing import Annotated

from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict

from tracker.api.dependencies import RunRuntimeDependency, TrackedBenchmarkId
from tracker.api.download import local_file_response, resolve_download_url
from tracker.api.storage_errors import storage_errors
from tracker.runtime.artifacts import benchmark_prefix, run_attempts_prefix

router = APIRouter(prefix="/benchmarks")


class RunArtifactEntry(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"description": "An object stored beneath one run's artifact directory."}
    )
    path: str
    size: int
    last_modified: datetime | None = None


class RunArtifactsResponse(BaseModel):
    model_config = ConfigDict(json_schema_extra={"description": "One page of run artifacts."})
    artifacts: list[RunArtifactEntry]
    next_cursor: str | None = None


class RunArtifactDownloadResponse(BaseModel):
    model_config = ConfigDict(json_schema_extra={"description": "A temporary URL for a single run artifact."})
    path: str
    download_url: str
    expires_in: int
    size: int


AttemptsQuery = Annotated[bool, Query(description="Read artifacts kept from task attempts replaced by a retry.")]


def _root(benchmark_id: str, attempts: bool) -> str:
    return run_attempts_prefix(benchmark_id) if attempts else benchmark_prefix(benchmark_id)


def _path(value: str, *, allow_empty: bool = False) -> str:
    if not value and allow_empty:
        return value
    if "\\" in value or any(part in {"", ".", ".."} for part in value.split("/")):
        raise HTTPException(status_code=400, detail="Artifact path must be a relative file or directory path")
    return value


@router.get(
    "/{benchmark_id}/artifacts",
    response_model=RunArtifactsResponse,
    description="List an exact artifact path or its descendants within the authorized run.",
)
async def list_run_artifacts(
    benchmark_id: TrackedBenchmarkId,
    run_context: RunRuntimeDependency,
    prefix: str = "",
    cursor: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    attempts: AttemptsQuery = False,
) -> RunArtifactsResponse:
    prefix = _path(prefix.rstrip("/"), allow_empty=True)
    root = _root(str(benchmark_id), attempts)
    with storage_errors("Artifact"):
        objects, next_cursor = await run_context.objects.list_objects_page(root + prefix, cursor=cursor, limit=limit)
    entries = [
        RunArtifactEntry(path=path, size=item.size, last_modified=item.last_modified)
        for item in objects
        if not item.key.endswith("/")
        and (path := item.key.removeprefix(root))
        and (not prefix or path == prefix or path.startswith(prefix + "/"))
    ]
    return RunArtifactsResponse(artifacts=entries, next_cursor=next_cursor)


@router.get(
    "/{benchmark_id}/artifacts/download-url",
    response_model=RunArtifactDownloadResponse,
    description="Return a temporary download URL for an exact artifact in the authorized run.",
)
async def get_run_artifact_url(
    benchmark_id: TrackedBenchmarkId,
    run_context: RunRuntimeDependency,
    request: Request,
    path: str = Query(min_length=1),
    download: bool = False,
    attempts: AttemptsQuery = False,
) -> RunArtifactDownloadResponse | FileResponse:
    path = _path(path)
    key = _root(str(benchmark_id), attempts) + path
    with storage_errors("Artifact"):
        metadata = await run_context.objects.stat(key)
        if download:
            response = local_file_response(run_context.objects, key, filename=Path(path).name)
            if response is not None:
                return response
        url, ttl = await resolve_download_url(
            run_context.objects,
            key,
            request=request,
            route_name="get_run_artifact_url",
            route_params={"benchmark_id": benchmark_id},
            query_params={"path": path, "attempts": "true"} if attempts else {"path": path},
            expires_in=300,
        )
    return RunArtifactDownloadResponse(path=path, download_url=url, expires_in=ttl, size=metadata.size)
