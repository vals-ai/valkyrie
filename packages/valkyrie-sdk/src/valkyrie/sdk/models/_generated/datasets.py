from __future__ import annotations
from valkyrie.sdk.models._base import ResponseModel, UTCDateTime


class DatasetTaskResponse(ResponseModel):
    """One task archive stored for a dataset."""

    task_id: str
    size: int
    last_modified: UTCDateTime | None = None


class ResourceResponse(ResponseModel):
    """What a benchmark and a dataset have in common."""

    name: str
    description: str
    created_by_email: str | None = None
    created_at: UTCDateTime
    updated_at: UTCDateTime
    archived_at: UTCDateTime | None = None


class BenchmarkDefinitionResponse(ResourceResponse):
    """One saved benchmark."""


class BenchmarkDefinitionsResponse(ResponseModel):
    """The organization's active benchmarks, by name."""

    benchmarks: list[BenchmarkDefinitionResponse]


class DatasetResponse(ResourceResponse):
    """One saved dataset and the name of the benchmark it belongs to."""

    benchmark: str


class DatasetTaskDownloadResponse(DatasetTaskResponse):
    """A task and a temporary URL to download its archive."""

    download_url: str
    expires_in: int


class DatasetTasksResponse(ResponseModel):
    """A page of a dataset's tasks, and where the next page starts."""

    tasks: list[DatasetTaskResponse]
    next_cursor: str | None = None


class DatasetsResponse(ResponseModel):
    """A benchmark's active datasets, by name."""

    datasets: list[DatasetResponse]
