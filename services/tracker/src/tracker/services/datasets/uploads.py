import hashlib
import io
import os
import shutil
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from valkyrie.sdk.dataset_manifest import check_digests, check_script, lock_folders, read_manifest, wire_manifest
from valkyrie.sdk.metric_script import read_metric_script
from valkyrie.sdk.task_archive import find_task_folders, pack_task, task_problems
from valkyrie.sdk.task_content import TASK_FOLDERS, left_out

from tracker import config
from tracker.services.datasets.digest import archive_digest
from tracker.services.datasets.extract import ArchiveTooLargeError, extract_upload
from tracker.services.datasets.folder import COMPOSE_FILE, CONFIG_FILE
from tracker.services.datasets.metric_script import MAX_METRIC_BYTES, check_metric_script, file_digest
from tracker.services.datasets.publish import InvalidUploadError, RejectedUploadError, UploadTooLargeError
from tracker.services.datasets.validate import validate_archive
from tracker.services.datasets.source import TASK_FILES
from tracker.types import VersionCreateRequest, VersionManifest

_DIRECTORY_READ_BYTES = 32 * 1024**2
_MAX_CONFIG_BYTES = 1024**2
_MAX_MANIFEST_BYTES = 32 * 1024**2
_MAX_PACKED_BYTES = 6 * 1024**3
_DESKTOP_METADATA = {".DS_Store", "__MACOSX"}


@dataclass(frozen=True)
class PreparedDataset:
    request: VersionCreateRequest
    archives: dict[str, Path]
    warnings: list[str]
    metric: bytes | None


class _ArchiveReader(io.RawIOBase):
    def __init__(self, source: BinaryIO, size: int) -> None:
        super().__init__()
        self._source = source
        self._size = size
        self._remaining: int | None = _DIRECTORY_READ_BYTES

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._source.seek(offset, whence)

    def tell(self) -> int:
        return self._source.tell()

    def read(self, size: int = -1) -> bytes:
        length = max(0, self._size - self.tell()) if size < 0 else min(size, max(0, self._size - self.tell()))
        if self._remaining is not None:
            if length > self._remaining:
                raise UploadTooLargeError("The ZIP directory exceeds the inspection limit")
            self._remaining -= length
        return self._source.read(length)

    def allow_contents(self) -> None:
        self._remaining = None


def _unpack(source: BinaryIO, size: int, destination: Path) -> None:
    reader = _ArchiveReader(source, size)
    with reader, zipfile.ZipFile(reader) as archive:
        reader.allow_contents()
        try:
            extract_upload(archive, destination)
        except ArchiveTooLargeError as error:
            raise UploadTooLargeError("The unpacked archive exceeds the size limit") from error


def _root(directory: Path) -> Path:
    root = directory
    while not (root / "dataset.toml").exists() and not (root / "metric.py").exists():
        children = [child for child in root.iterdir() if child.name not in _DESKTOP_METADATA]
        if len(children) != 1 or not children[0].is_dir() or (children[0] / CONFIG_FILE).exists():
            break
        root = children[0]
    return root


def _limit_file(path: Path, maximum: int) -> None:
    if path.is_file() and path.stat().st_size > maximum:
        raise UploadTooLargeError(f"The configuration file {path.name!r} exceeds the size limit")


def _warn(warnings: list[str], message: str) -> None:
    if len(warnings) < 199:
        warnings.append(message[:2000])
    elif len(warnings) == 199:
        warnings.append("Additional warnings are omitted")


def _prepare(root: Path, packed: Path, target_name: str) -> PreparedDataset:
    folders = find_task_folders(root)
    for folder in folders.values():
        _limit_file(folder / CONFIG_FILE, _MAX_CONFIG_BYTES)
    _limit_file(root / "dataset.toml", _MAX_MANIFEST_BYTES)
    _limit_file(root / "metric.py", MAX_METRIC_BYTES)
    manifest = read_manifest(root)
    metric = read_metric_script(root)
    check_script(manifest, metric)
    if metric is not None:
        if not config.METRIC_RUNNER_FUNCTION:
            raise RejectedUploadError("Custom scoring isn't available in this Valkyrie deployment")
        check_metric_script(metric)
    locked = None if manifest is None else lock_folders(manifest, folders, root)
    warnings: list[str] = []
    if locked is not None:
        folders = locked.folders
        if locked.unlisted:
            _warn(
                warnings,
                f"dataset.toml excludes {len(locked.unlisted)} task folders: {', '.join(locked.unlisted[:20])}",
            )
        if locked.manifest.dataset.name != target_name:
            _warn(
                warnings,
                f"dataset.toml names {locked.manifest.dataset.name}. The publication destination is {target_name}",
            )
    archives: dict[str, Path] = {}
    digests: dict[str, str] = {}
    total_packed = 0
    for task_id, folder in folders.items():
        for name in TASK_FOLDERS:
            path = folder / name
            if path.exists() and not path.is_dir():
                raise RejectedUploadError(f"Task {task_id}: {name} must be a directory")
        _limit_file(folder / ".gitignore", _MAX_CONFIG_BYTES)
        for ignore_file in folder.rglob(".harborignore"):
            _limit_file(ignore_file, _MAX_CONFIG_BYTES)
        for pattern in TASK_FILES:
            if pattern.endswith(COMPOSE_FILE):
                for configuration in folder.glob(pattern):
                    _limit_file(configuration, _MAX_CONFIG_BYTES)
        if problems := task_problems(folder):
            raise RejectedUploadError(f"Task {task_id} is not valid: {'; '.join(problems[:20])}")
        if omitted := left_out(folder):
            _warn(warnings, f"Task {task_id} omits {len(omitted)} root entries: {', '.join(omitted[:20])}")
        path = packed / f"{task_id}.zip"
        pack_task(folder, path)
        size = path.stat().st_size
        total_packed += size
        if size > config.DATASET_TASK_UPLOAD_MAX_BYTES or total_packed > _MAX_PACKED_BYTES:
            raise UploadTooLargeError("The packed tasks exceed the upload size limits")
        with zipfile.ZipFile(path) as archive:
            digest = archive_digest(archive)
            report = validate_archive(archive)
        if not report.ok:
            raise RejectedUploadError(f"Task {task_id} is not valid: {'; '.join(report.errors[:20])}")
        for warning in report.warnings:
            _warn(warnings, f"Task {task_id}: {warning}")
        digests[task_id] = digest
        if digest in archives:
            path.unlink()
        else:
            archives[digest] = path
    if locked is not None:
        check_digests(locked, digests)
    request = VersionCreateRequest(
        tasks=digests,
        manifest=None
        if locked is None
        else VersionManifest.model_validate(wire_manifest(locked, digests).model_dump()),
        tags=[] if locked is None or locked.tag is None else [locked.tag],
        metric_digest=None if metric is None else file_digest(metric),
    )
    return PreparedDataset(request, archives, warnings, metric)


def prepare_dataset_archive(
    archive_path: Path, destination: Path, *, expected_size: int, expected_sha256: str, target_name: str
) -> PreparedDataset:
    if not 1 <= expected_size <= config.DATASET_ARCHIVE_UPLOAD_MAX_BYTES:
        raise UploadTooLargeError("The archive exceeds the upload size limit")
    destination.mkdir()
    unpacked, packed = destination / "unpacked", destination / "packed"
    unpacked.mkdir()
    packed.mkdir()
    try:
        with archive_path.open("rb") as source:
            size = os.fstat(source.fileno()).st_size
            if size != expected_size:
                raise InvalidUploadError("The uploaded archive size does not match the upload request")
            if hashlib.file_digest(source, "sha256").hexdigest() != expected_sha256:
                raise InvalidUploadError("The uploaded archive checksum does not match the upload request")
            source.seek(0)
            try:
                _unpack(source, size, unpacked)
            except (zlib.error, EOFError) as error:
                raise InvalidUploadError("The ZIP archive contains damaged compressed data") from error
        prepared = _prepare(_root(unpacked), packed, target_name)
        shutil.rmtree(unpacked)
        return prepared
    except BaseException:
        shutil.rmtree(destination)
        raise
