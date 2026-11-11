import fnmatch
import os
import stat
import zipfile
from collections.abc import Collection
from pathlib import Path, PurePosixPath

MAX_ENTRIES = 100_000
MAX_EXTRACTED_BYTES = 5 * 1024**3
ALLOWED_COMPRESSION = (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
ALLOWED_FILE_TYPES = (0, stat.S_IFREG, stat.S_IFDIR)


class UnsafeArchiveError(ValueError):
    pass


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
    if sum(entry.file_size for entry in selected) > MAX_EXTRACTED_BYTES:
        raise UnsafeArchiveError(f"The files extracted exceed {MAX_EXTRACTED_BYTES} bytes")
    for entry in selected:
        extracted = archive.extract(entry, destination)
        mode = entry.external_attr >> 16 & 0o777
        if mode and not entry.is_dir():
            os.chmod(extracted, mode | stat.S_IRUSR | stat.S_IWUSR)
