"""Read a dataset.toml, pair its tasks with task folders, and lock their digests."""

import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator

from valkyrie.sdk.metric_script import METRIC_FILE, metric_digest
from valkyrie.sdk.models.datasets import (
    DIGEST_PATTERN,
    ManifestAuthor,
    ManifestTask,
    VersionManifest,
    validate_tag,
)

MANIFEST_FILE = "dataset.toml"
ORG_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]*/[a-zA-Z0-9][a-zA-Z0-9._-]*")


class ManifestError(ValueError):
    pass


def _org_name(value: str, kind: str) -> str:
    if not ORG_NAME.fullmatch(value) or ".." in value:
        raise ValueError(f"{kind} name must be in 'org/name' format, such as acme/swe-bench. Got: {value}")
    return value


class _Task(BaseModel):
    name: str
    digest: str

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        return _org_name(value, "Task")

    @field_validator("digest")
    @classmethod
    def valid_digest(cls, value: str) -> str:
        if not DIGEST_PATTERN.fullmatch(value):
            raise ValueError(f"Digest must be in 'sha256:<64 hex chars>' format. Got: {value}")
        return value


class _File(BaseModel):
    path: str
    digest: str = ""

    @field_validator("digest")
    @classmethod
    def valid_digest(cls, value: str) -> str:
        if value and not DIGEST_PATTERN.fullmatch(value):
            raise ValueError(f"Digest must be in 'sha256:<64 hex chars>' format. Got: {value}")
        return value


class _Dataset(BaseModel):
    name: str
    version: str | None = Field(default=None, min_length=1)
    description: str = ""
    authors: list[ManifestAuthor] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        return _org_name(value, "Dataset")


class DatasetToml(BaseModel):
    dataset: _Dataset
    metadata: dict[str, Any] = Field(default_factory=dict)
    tasks: list[_Task] = Field(default_factory=list)
    files: list[_File] = Field(default_factory=list)


@dataclass(frozen=True)
class LockedDataset:
    """The task folders a dataset.toml selects and what it says about them."""

    folders: dict[str, Path]
    source_names: dict[str, str]
    unlisted: list[str]
    manifest: DatasetToml
    tag: str | None


def read_manifest(root: Path) -> DatasetToml | None:
    """Parse ROOT/dataset.toml when it exists, refusing a manifest Harbor would refuse and one that pins files."""
    path = root / MANIFEST_FILE
    if not path.is_file():
        return None
    try:
        manifest = DatasetToml.model_validate(tomllib.loads(path.read_text(encoding="utf-8")))
    except (tomllib.TOMLDecodeError, ValidationError) as error:
        raise ManifestError(f"{path} is not a valid dataset.toml: {error}") from error
    if unsupported := [file.path for file in manifest.files if file.path != METRIC_FILE]:
        raise ManifestError(
            f"{path} pins dataset files ({', '.join(unsupported)}), but only {METRIC_FILE} is supported"
        )
    if not manifest.tasks:
        raise ManifestError(f"{path} lists no tasks")
    return manifest


def check_script(manifest: DatasetToml | None, script: bytes | None) -> None:
    """Refuse a manifest that pins metric.py when the folder has none or has a different one."""
    if manifest is None:
        return
    for file in manifest.files:
        if script is None:
            raise ManifestError(f"dataset.toml pins {METRIC_FILE} but the folder has none")
        found = metric_digest(script)
        if file.digest and file.digest != found:
            raise ManifestError(
                f"{METRIC_FILE} changed since dataset.toml was written: it lists {file.digest} but the file has {found}"
            )


def _task_name(folder: Path) -> str | None:
    try:
        section = tomllib.loads((folder / "task.toml").read_text(encoding="utf-8")).get("task")
    except (OSError, tomllib.TOMLDecodeError):
        return None
    name = section.get("name") if isinstance(section, dict) else None
    return name if isinstance(name, str) else None


def lock_folders(manifest: DatasetToml, folders: Mapping[str, Path], root: Path) -> LockedDataset:
    """Pair each listed task with the folder whose task.toml [task] name is the same."""
    names = {task_id: _task_name(path) for task_id, path in folders.items()}
    if unnamed := sorted(task_id for task_id, name in names.items() if name is None):
        raise ManifestError(
            f"{root / MANIFEST_FILE} lists tasks by name, but these folders have no [task] name in task.toml: "
            f"{', '.join(unnamed)}"
        )
    by_name: dict[str, list[str]] = {}
    for task_id, name in names.items():
        by_name.setdefault(str(name), []).append(task_id)
    if duplicated := {name: ids for name, ids in by_name.items() if len(ids) > 1}:
        raise ManifestError(
            "Two task folders have the same [task] name: "
            + "; ".join(f"{name} in {', '.join(ids)}" for name, ids in sorted(duplicated.items()))
        )

    listed: dict[str, str] = {}
    for task in manifest.tasks:
        if listed.setdefault(task.name, task.digest) != task.digest:
            raise ManifestError(f"dataset.toml lists {task.name} with two different digests")
    if missing := sorted(name for name in listed if name not in by_name):
        raise ManifestError(
            f"dataset.toml lists tasks that no folder under {root} has a [task] name for: {', '.join(missing)}"
        )

    chosen = {by_name[name][0]: name for name in listed}
    version = manifest.dataset.version
    if version is not None:
        try:
            validate_tag(version)
        except ValueError as error:
            raise ManifestError(f"The dataset.toml version {version!r} can't be a tag: {error}") from error
    return LockedDataset(
        folders={task_id: folders[task_id] for task_id in sorted(chosen)},
        source_names={task_id: chosen[task_id] for task_id in sorted(chosen)},
        unlisted=sorted(set(folders) - set(chosen)),
        manifest=manifest,
        tag=version,
    )


def check_digests(locked: LockedDataset, digests: Mapping[str, str]) -> None:
    """Refuse a task whose files no longer hash to the digest the manifest lists for it."""
    listed = {task.name: task.digest for task in locked.manifest.tasks}
    for task_id, source_name in locked.source_names.items():
        if digests[task_id] != listed[source_name]:
            raise ManifestError(
                f"Task {task_id} ({source_name}) changed since dataset.toml was written: "
                f"it lists {listed[source_name]} but the files have {digests[task_id]}"
            )


def wire_manifest(locked: LockedDataset, digests: Mapping[str, str]) -> VersionManifest:
    info = locked.manifest.dataset
    return VersionManifest(
        name=info.name,
        version=info.version,
        description=info.description,
        authors=info.authors,
        keywords=info.keywords,
        metadata=locked.manifest.metadata,
        tasks={
            task_id: ManifestTask(name=source_name, digest=digests[task_id])
            for task_id, source_name in locked.source_names.items()
        },
    )
