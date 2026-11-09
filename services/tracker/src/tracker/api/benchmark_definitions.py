from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException

from tracker.auth import RequestIdentity, get_current_org, get_current_starter
from tracker.database.models import BenchmarkDefinition, Org
from tracker.exceptions import BenchmarkHasDatasetsError, BenchmarkNameTakenError, ResourceArchivedError
from tracker.repositories.benchmark_definition import BenchmarkDefinitionRepositoryDependency
from tracker.types import (
    BenchmarkDefinitionResponse,
    BenchmarkDefinitionsResponse,
    ResourceCreateRequest,
    ResourceName,
    ResourceUpdateRequest,
)

router = APIRouter(prefix="/benchmarks")


NOT_FOUND: dict[int | str, dict[str, Any]] = {404: {"description": "The organization has no benchmark with this name"}}
ARCHIVED: dict[int | str, dict[str, Any]] = {409: {"description": "The benchmark is archived; restore it first"}}


def _not_found(name: str) -> HTTPException:
    return HTTPException(status_code=404, detail=f"Benchmark '{name}' not found")


def get_benchmark_definition(
    benchmark: ResourceName,
    repository: BenchmarkDefinitionRepositoryDependency,
    org: Org = Depends(get_current_org),
) -> BenchmarkDefinition:
    row = repository.get(org.id, benchmark, "share")
    if row is None:
        raise _not_found(benchmark)
    return row


BenchmarkDefinitionDependency = Annotated[BenchmarkDefinition, Depends(get_benchmark_definition)]


def get_active_benchmark_definition(benchmark: BenchmarkDefinitionDependency) -> BenchmarkDefinition:
    if benchmark.archived_at is not None:
        raise HTTPException(status_code=409, detail=str(ResourceArchivedError("Benchmark", benchmark.name)))
    return benchmark


ActiveBenchmarkDefinitionDependency = Annotated[BenchmarkDefinition, Depends(get_active_benchmark_definition)]


@router.post(
    "",
    response_model=BenchmarkDefinitionResponse,
    status_code=201,
    description="Create a benchmark. Its name is unique among the organization's benchmarks, archived ones included.",
    responses={409: {"description": "The organization already has a benchmark with this name"}},
)
def create_benchmark_definition(
    request: ResourceCreateRequest,
    repository: BenchmarkDefinitionRepositoryDependency,
    identity: RequestIdentity = Depends(get_current_starter),
) -> BenchmarkDefinitionResponse:
    try:
        benchmark = repository.create(identity.org.id, request.name, request.description, identity.email)
    except BenchmarkNameTakenError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return BenchmarkDefinitionResponse.from_row(benchmark)


@router.get(
    "",
    response_model=BenchmarkDefinitionsResponse,
    description="List the organization's benchmarks by name, leaving out archived ones unless include_archived is set.",
)
def list_benchmark_definitions(
    repository: BenchmarkDefinitionRepositoryDependency,
    org: Org = Depends(get_current_org),
    include_archived: bool = False,
) -> BenchmarkDefinitionsResponse:
    return BenchmarkDefinitionsResponse(
        benchmarks=[BenchmarkDefinitionResponse.from_row(row) for row in repository.list_all(org.id, include_archived)]
    )


@router.get(
    "/{benchmark}",
    response_model=BenchmarkDefinitionResponse,
    description="Return one of the organization's benchmarks, archived or not.",
    responses=NOT_FOUND,
)
def get_benchmark_definition_by_name(benchmark: BenchmarkDefinitionDependency) -> BenchmarkDefinitionResponse:
    return BenchmarkDefinitionResponse.from_row(benchmark)


@router.patch(
    "/{benchmark}",
    response_model=BenchmarkDefinitionResponse,
    description="Change a benchmark's description.",
    responses={**NOT_FOUND, **ARCHIVED},
)
def update_benchmark_definition(
    benchmark: ResourceName,
    request: ResourceUpdateRequest,
    repository: BenchmarkDefinitionRepositoryDependency,
    org: Org = Depends(get_current_org),
) -> BenchmarkDefinitionResponse:
    try:
        row = repository.update_description(org.id, benchmark, request.description)
    except ResourceArchivedError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    if row is None:
        raise _not_found(benchmark)
    return BenchmarkDefinitionResponse.from_row(row)


@router.delete(
    "/{benchmark}",
    response_model=BenchmarkDefinitionResponse,
    description="Archive a benchmark and return it. It stays readable and keeps its name, and restore brings it back.",
    responses={
        **NOT_FOUND,
        409: {"description": "The benchmark still has active datasets; archive them first"},
    },
)
def archive_benchmark_definition(
    benchmark: ResourceName,
    repository: BenchmarkDefinitionRepositoryDependency,
    org: Org = Depends(get_current_org),
) -> BenchmarkDefinitionResponse:
    try:
        row = repository.archive(org.id, benchmark)
    except BenchmarkHasDatasetsError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    if row is None:
        raise _not_found(benchmark)
    return BenchmarkDefinitionResponse.from_row(row)


@router.post(
    "/{benchmark}/restore",
    response_model=BenchmarkDefinitionResponse,
    description="Restore an archived benchmark and return it. A benchmark that is not archived is returned as it is.",
    responses=NOT_FOUND,
)
def restore_benchmark_definition(
    benchmark: ResourceName,
    repository: BenchmarkDefinitionRepositoryDependency,
    org: Org = Depends(get_current_org),
) -> BenchmarkDefinitionResponse:
    row = repository.restore(org.id, benchmark)
    if row is None:
        raise _not_found(benchmark)
    return BenchmarkDefinitionResponse.from_row(row)
