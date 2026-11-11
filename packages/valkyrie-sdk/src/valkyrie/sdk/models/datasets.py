"""Benchmarks an organization owns and the datasets that belong to them."""

import re
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, StringConstraints

from valkyrie.sdk.errors import ValkyrieRunError

from valkyrie.sdk.models._generated.datasets import (
    BenchmarkDefinitionResponse,
    BenchmarkDefinitionsResponse,
    BlobResponse,
    DatasetResponse,
    DatasetTaskDownloadResponse,
    DatasetTaskResponse,
    DatasetTasksPage,
    DatasetVersionDetailResponse,
    DatasetVersionResponse,
    DatasetVersionsResponse,
    DatasetsResponse,
    HostedDataset,
    LocalDataset,
    ManifestAuthor,
    ManifestFile,
    ManifestInfo,
    ManifestTask,
    MissingBlobsResponse,
    ResourceResponse,
    VersionManifest,
)

__all__ = [
    "BenchmarkDefinitionResponse",
    "BenchmarkDefinitionsResponse",
    "BlobResponse",
    "DatasetLocation",
    "DatasetRef",
    "DatasetResponse",
    "DatasetTaskDownloadResponse",
    "DatasetTaskResponse",
    "DatasetTasksPage",
    "DatasetVersionDetailResponse",
    "DatasetVersionResponse",
    "DatasetVersionsResponse",
    "DatasetsResponse",
    "Digest",
    "HostedDataset",
    "LocalDataset",
    "ManifestAuthor",
    "ManifestFile",
    "ManifestInfo",
    "ManifestTask",
    "MissingBlobsResponse",
    "ResourceResponse",
    "VersionManifest",
    "validate_name",
    "folder_run_name",
    "validate_tag",
    "validate_task_id",
]

NAME_PATTERN = re.compile(r"[a-z0-9]+(?:[._-][a-z0-9]+)*")
NAME_MAX_LENGTH = 64
TASK_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
TASK_ID_MAX_LENGTH = 128
TAG_PATTERN = re.compile(r"[a-z0-9][a-z0-9.\-]*")
TAG_RULE = (
    "a tag is lowercase letters, digits, hyphens and dots, starts with a letter or digit, and is not a whole number"
)
DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")
Digest = Annotated[str, StringConstraints(pattern=f"^{DIGEST_PATTERN.pattern}$")]


def validate_name(name: str) -> str:
    if len(name) > NAME_MAX_LENGTH or not NAME_PATTERN.fullmatch(name):
        raise ValueError(
            "must be lowercase letters and digits with single '.', '_' or '-' separators, "
            f"at most {NAME_MAX_LENGTH} characters, e.g. swe-bench"
        )
    return name


def validate_task_id(task_id: str) -> str:
    if len(task_id) > TASK_ID_MAX_LENGTH or not TASK_ID_PATTERN.fullmatch(task_id):
        raise ValueError(
            "must start with a letter or digit and use only letters, digits, '.', '_' and '-', "
            f"at most {TASK_ID_MAX_LENGTH} characters, e.g. django__django-11099"
        )
    return task_id


def folder_run_name(path: str | Path) -> str:
    """A run name for a folder of tasks: its name in lowercase letters and digits with single dashes."""
    name = re.sub(r"[^a-z0-9]+", "-", Path(path).resolve().name.lower()).strip("-")[:NAME_MAX_LENGTH].rstrip("-")
    if not name:
        raise ValkyrieRunError(f"Can't name a run after the folder {path}; pass a benchmark name")
    return name


def validate_tag(tag: str) -> str:
    if tag.isdigit() or tag.startswith("sha256:") or not TAG_PATTERN.fullmatch(tag):
        raise ValueError(f"Invalid tag {tag!r}: {TAG_RULE}")
    return tag


DatasetRef = Annotated[HostedDataset | LocalDataset, Field(discriminator="kind")]
DatasetLocation = Literal["harbor", "github", "hugging_face"]
