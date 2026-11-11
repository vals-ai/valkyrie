import hashlib
import zipfile
from collections.abc import Iterable, Mapping

from tracker.services.datasets.extract import MAX_EXTRACTED_BYTES, UnsafeArchiveError, check_archive

DIGEST_PREFIX = "sha256:"
DIGEST_PATTERN = rf"{DIGEST_PREFIX}[0-9a-f]{{64}}"


def content_digest(files: Iterable[tuple[str, str]]) -> str:
    outer = hashlib.sha256()
    for name, digest in sorted(files):
        outer.update(f"{name}\0{digest}\n".encode())
    return DIGEST_PREFIX + outer.hexdigest()


def archive_digest(archive: zipfile.ZipFile) -> str:
    check_archive(archive)
    files = [entry for entry in archive.infolist() if not entry.is_dir()]
    names = [entry.filename for entry in files]
    if len(set(names)) != len(names):
        raise UnsafeArchiveError("The archive holds a file more than once")
    if sum(entry.file_size for entry in files) > MAX_EXTRACTED_BYTES:
        raise UnsafeArchiveError(f"The files exceed {MAX_EXTRACTED_BYTES} bytes")
    pairs: list[tuple[str, str]] = []
    for entry in files:
        with archive.open(entry) as source:
            pairs.append((entry.filename, hashlib.file_digest(source, "sha256").hexdigest()))
    return content_digest(pairs)


def version_digest(tasks: Mapping[str, str], files: Mapping[str, str] | None = None) -> str:
    outer = hashlib.sha256()
    for task_id, digest in sorted(tasks.items()):
        outer.update(f"task\0{task_id}\0{digest}\n".encode())
    for path, digest in sorted((files or {}).items()):
        outer.update(f"file\0{path}\0{digest}\n".encode())
    return DIGEST_PREFIX + outer.hexdigest()
