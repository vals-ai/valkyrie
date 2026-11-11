from __future__ import annotations
from typing import Annotated, Any, Literal
from pydantic import BaseModel, Field, StringConstraints
from valkyrie.sdk.models._base import ResponseModel, UTCDateTime
from uuid import UUID


class DatasetTaskResponse(ResponseModel):
    """One task of a dataset version, with the digest of its files and the size of its archive."""

    task_id: str
    digest: Annotated[str, Field(pattern="^sha256:[0-9a-f]{64}$")]
    size: int
    source_name: str | None = None


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


class DatasetVersionResponse(ResponseModel):
    """One version of a dataset: a numbered revision holding exactly the tasks it was published with."""

    benchmark: str
    dataset: str
    revision: int
    digest: Annotated[str, Field(pattern="^sha256:[0-9a-f]{64}$")]
    task_count: int
    tags: list[str]
    created_by_email: str | None = None
    created_at: UTCDateTime


class DatasetTasksPage(ResponseModel):
    """One page of a dataset version's tasks in task ID order, and the cursor of the next page if any."""

    tasks: list[DatasetTaskResponse]
    next_cursor: str | None = None


class DatasetVersionsResponse(ResponseModel):
    """A dataset's versions, newest first."""

    versions: list[DatasetVersionResponse]


class BlobResponse(ResponseModel):
    """A task archive the dataset stores under the digest of its files, with what the Tracker warns about."""

    digest: Annotated[str, Field(pattern="^sha256:[0-9a-f]{64}$")]
    size: int
    warnings: list[str] = []


type Digest = Annotated[
    str,
    Field(
        description="A content digest written as sha256: and 64 lowercase hex digits.",
        pattern="^sha256:[0-9a-f]{64}$",
    ),
]


class MissingBlobsResponse(ResponseModel):
    """The digests among those asked about whose archive the dataset does not store yet."""

    missing: list[Digest]


class ManifestAuthor(ResponseModel):
    """An author named in a dataset.toml."""

    name: str
    email: str | None = None


class ManifestInfo(ResponseModel):
    """What a dataset.toml says about its dataset, as stored with a version."""

    name: str
    version: str | None = None
    description: str = ""
    authors: Annotated[list[ManifestAuthor], Field(validate_default=True)] = []
    keywords: list[str] = []
    metadata: dict[str, Any] = {}


class ManifestTask(BaseModel):
    """A task a dataset.toml lists, by the name in its task.toml, with the digest of its files."""

    name: str
    digest: Annotated[str, Field(pattern="^sha256:[0-9a-f]{64}$")]


class ManifestFile(BaseModel):
    """A dataset file a dataset.toml pins."""

    path: str
    digest: str = ""


class VersionManifest(ResponseModel):
    """A dataset.toml sent with a version, whose tasks are keyed by task ID."""

    name: str
    version: str | None = None
    description: str = ""
    authors: Annotated[list[ManifestAuthor], Field(validate_default=True)] = []
    keywords: list[str] = []
    metadata: dict[str, Any] = {}
    tasks: dict[
        Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")],
        ManifestTask,
    ]
    files: Annotated[list[ManifestFile], Field(validate_default=True)] = []


class HostedDataset(BaseModel):
    """A dataset stored in the Tracker, named by its benchmark and its own name."""

    kind: Literal["hosted"] = "hosted"
    benchmark: str
    dataset: str
    ref: str = "latest"
    dataset_id: UUID | None = None
    revision: int | None = None


class LocalDataset(BaseModel):
    """A folder of tasks on the machine of a local Tracker."""

    kind: Literal["folder"] = "folder"
    path: str


class DatasetsResponse(ResponseModel):
    """A benchmark's active datasets, by name."""

    datasets: list[DatasetResponse]


class DatasetVersionDetailResponse(DatasetVersionResponse):
    """A dataset version with the digest of its metric script and the dataset.toml it was published with, if any."""

    metric_digest: Annotated[str | None, Field(pattern="^sha256:[0-9a-f]{64}$")] = None
    manifest: ManifestInfo | None = None
