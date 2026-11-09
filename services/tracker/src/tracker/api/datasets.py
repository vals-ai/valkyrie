from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from tracker.api.benchmark_definitions import ActiveBenchmarkDefinitionDependency, BenchmarkDefinitionDependency
from tracker.auth import RequestIdentity, get_current_starter
from tracker.exceptions import DatasetNameTakenError, ResourceArchivedError
from tracker.repositories.dataset import DatasetRepositoryDependency
from tracker.types import (
    DatasetResponse,
    DatasetsResponse,
    ResourceCreateRequest,
    ResourceName,
    ResourceUpdateRequest,
)

router = APIRouter(prefix="/benchmarks/{benchmark}/datasets")


NOT_FOUND: dict[int | str, dict[str, Any]] = {
    404: {"description": "The organization has no benchmark with this name, or it has no dataset with this name"}
}
ARCHIVED: dict[int | str, dict[str, Any]] = {409: {"description": "The dataset is archived; restore it first"}}


def _not_found(name: str) -> HTTPException:
    return HTTPException(status_code=404, detail=f"Dataset '{name}' not found")


@router.post(
    "",
    response_model=DatasetResponse,
    status_code=201,
    description="Create a dataset. Its name is unique among the benchmark's datasets, archived ones included.",
    responses={
        404: NOT_FOUND[404],
        409: {"description": "The benchmark is archived, or already has a dataset with this name"},
    },
)
def create_dataset(
    benchmark: ActiveBenchmarkDefinitionDependency,
    request: ResourceCreateRequest,
    repository: DatasetRepositoryDependency,
    identity: RequestIdentity = Depends(get_current_starter),
) -> DatasetResponse:
    try:
        dataset = repository.create(benchmark.id, request.name, request.description, identity.email)
    except DatasetNameTakenError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return DatasetResponse.from_row(dataset, benchmark.name)


@router.get(
    "",
    response_model=DatasetsResponse,
    description="List the benchmark's datasets by name, leaving out archived ones unless include_archived is set.",
    responses={404: NOT_FOUND[404]},
)
def list_datasets(
    benchmark: BenchmarkDefinitionDependency,
    repository: DatasetRepositoryDependency,
    include_archived: bool = False,
) -> DatasetsResponse:
    return DatasetsResponse(
        datasets=[
            DatasetResponse.from_row(row, benchmark.name) for row in repository.list_all(benchmark.id, include_archived)
        ]
    )


@router.get(
    "/{name}",
    response_model=DatasetResponse,
    description="Return one of the benchmark's datasets, archived or not.",
    responses=NOT_FOUND,
)
def get_dataset(
    benchmark: BenchmarkDefinitionDependency, name: ResourceName, repository: DatasetRepositoryDependency
) -> DatasetResponse:
    dataset = repository.get(benchmark.id, name)
    if dataset is None:
        raise _not_found(name)
    return DatasetResponse.from_row(dataset, benchmark.name)


@router.patch(
    "/{name}",
    response_model=DatasetResponse,
    description="Change a dataset's description.",
    responses={**NOT_FOUND, **ARCHIVED},
)
def update_dataset(
    benchmark: BenchmarkDefinitionDependency,
    name: ResourceName,
    request: ResourceUpdateRequest,
    repository: DatasetRepositoryDependency,
) -> DatasetResponse:
    try:
        dataset = repository.update_description(benchmark.id, name, request.description)
    except ResourceArchivedError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    if dataset is None:
        raise _not_found(name)
    return DatasetResponse.from_row(dataset, benchmark.name)


@router.delete(
    "/{name}",
    response_model=DatasetResponse,
    description="Archive a dataset and return it. It stays readable and keeps its name, and restore brings it back.",
    responses=NOT_FOUND,
)
def archive_dataset(
    benchmark: BenchmarkDefinitionDependency, name: ResourceName, repository: DatasetRepositoryDependency
) -> DatasetResponse:
    dataset = repository.archive(benchmark.id, name)
    if dataset is None:
        raise _not_found(name)
    return DatasetResponse.from_row(dataset, benchmark.name)


@router.post(
    "/{name}/restore",
    response_model=DatasetResponse,
    description="Restore an archived dataset and return it. A dataset that is not archived is returned as it is.",
    responses={
        **NOT_FOUND,
        409: {"description": "The benchmark is archived; restore it first"},
    },
)
def restore_dataset(
    benchmark: ActiveBenchmarkDefinitionDependency, name: ResourceName, repository: DatasetRepositoryDependency
) -> DatasetResponse:
    dataset = repository.restore(benchmark.id, name)
    if dataset is None:
        raise _not_found(name)
    return DatasetResponse.from_row(dataset, benchmark.name)
