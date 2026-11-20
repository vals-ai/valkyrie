import fnmatch
import stat
import zipfile
from collections.abc import Collection
from pathlib import Path, PurePosixPath
from typing import Literal

_COPY_CHUNK_BYTES = 1024**2
MAX_ENTRIES = 100_000
MAX_EXTRACTED_BYTES = 5 * 1024**3
ALLOWED_COMPRESSION = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
ALLOWED_FILE_TYPES = (0, stat.S_IFREG, stat.S_IFDIR)


class UnsafeArchiveError(ValueError):
    pass


class ArchiveTooLargeError(UnsafeArchiveError):
    pass


def _check_size(size: int) -> None:
    if size > MAX_EXTRACTED_BYTES:
        raise ArchiveTooLargeError(f"The files extracted exceed {MAX_EXTRACTED_BYTES} bytes")


def check_archive(archive: zipfile.ZipFile) -> None:
    entries = archive.infolist()
    if len(entries) > MAX_ENTRIES:
        raise UnsafeArchiveError(f"The archive holds {len(entries)} entries, over the {MAX_ENTRIES} allowed")
    for entry in entries:
        name = entry.filename
        parts = PurePosixPath(name).parts
        if name.startswith("/") or "\\" in name or ":" in name or ".." in parts:
            raise UnsafeArchiveError(f"The archive holds an unsafe path: {name!r}")
        if stat.S_IFMT(entry.external_attr >> 16) not in ALLOWED_FILE_TYPES:
            raise UnsafeArchiveError(f"The archive holds a link or special file: {name!r}")
        if entry.compress_type not in ALLOWED_COMPRESSION:
            raise UnsafeArchiveError(f"The archive compresses {name!r} in a way that isn't accepted")


def extract(archive: zipfile.ZipFile, prefixes: Collection[str], destination: Path) -> None:
    check_archive(archive)
    selected = [
        entry
        for entry in archive.infolist()
        if any(
            entry.filename == prefix
            or entry.filename.startswith(f"{prefix.rstrip('/')}/")
            or fnmatch.fnmatchcase(entry.filename, prefix)
            for prefix in prefixes
        )
    ]
    seen: set[str] = set()
    for entry in selected:
        if entry.filename in seen:
            raise UnsafeArchiveError(f"The archive holds {entry.filename!r} more than once")
        seen.add(entry.filename)
    _check_size(sum(entry.file_size for entry in selected))
    _write_entries(archive, selected, destination, mode="wb")


def extract_upload(archive: zipfile.ZipFile, destination: Path) -> None:
    check_archive(archive)
    entries: list[zipfile.ZipInfo] = []
    names: set[str] = set()
    files: set[str] = set()
    expanded = 0
    for entry in archive.infolist():
        path = PurePosixPath(entry.filename)
        normalized = path.as_posix() + ("/" if entry.is_dir() else "")
        if (
            not path.parts
            or entry.orig_filename != entry.filename
            or entry.filename != normalized
            or len(path.parts) > 64
        ):
            raise UnsafeArchiveError(f"The ZIP contains an ambiguous or excessively deep path: {entry.filename!r}")
        if entry.flag_bits & 1:
            raise UnsafeArchiveError("Encrypted ZIP files are not supported")
        name = entry.filename.rstrip("/")
        if name in names:
            if entry.is_dir() and name not in files:
                continue
            raise UnsafeArchiveError(f"The ZIP contains a duplicate path: {name!r}")
        names.add(name)
        if not entry.is_dir():
            files.add(name)
            expanded += entry.file_size
        _check_size(expanded)
        entries.append(entry)
    if any(parent.as_posix() in files for name in names for parent in PurePosixPath(name).parents):
        raise UnsafeArchiveError("The ZIP uses a file path as a directory")
    _write_entries(archive, entries, destination, mode="xb")


def _write_entries(
    archive: zipfile.ZipFile, entries: list[zipfile.ZipInfo], destination: Path, *, mode: Literal["xb", "wb"]
) -> None:
    copied = 0
    for entry in entries:
        target = destination / entry.filename
        if entry.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        with archive.open(entry) as content, target.open(mode) as sink:
            while chunk := content.read(_COPY_CHUNK_BYTES):
                copied += len(chunk)
                _check_size(copied)
                sink.write(chunk)
        permissions = entry.external_attr >> 16 & 0o777
        if permissions:
            target.chmod(permissions | stat.S_IRUSR | stat.S_IWUSR)
