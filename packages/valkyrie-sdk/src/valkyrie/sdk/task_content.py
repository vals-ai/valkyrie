"""Which files make up a task, and the digest that names their content."""

import hashlib
import zipfile
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path

from pathspec import GitIgnoreSpec, PathSpec
from pathspec.pattern import Pattern

DIGEST_PREFIX = "sha256:"
TASK_FILES = ("task.toml", "instruction.md", "README.md", "trajectory.json")
TASK_FOLDERS = ("environment", "tests", "solution", "steps")
ROOT_IGNORE_FILE = ".gitignore"
DEFAULT_IGNORES = ("__pycache__/", "*.pyc", ".DS_Store", "*.swp", "*.swo", "*~")
IGNORE_FILE = ".harborignore"
IGNORE_SCOPES = GitIgnoreSpec.from_lines(("/tests/", "/solution/", "/steps/*/tests/", "/steps/*/solution/"))
READ_CHUNK_BYTES = 1024 * 1024


def _lines(path: Path) -> list[str]:
    if not path.is_file() or path.is_symlink():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()


class _IgnoreFiles:
    def __init__(self, folder: Path) -> None:
        self._folder = folder
        self._specs: dict[Path, GitIgnoreSpec] = {}

    def _spec(self, directory: Path) -> GitIgnoreSpec:
        if directory not in self._specs:
            self._specs[directory] = GitIgnoreSpec.from_lines(_lines(directory / IGNORE_FILE))
        return self._specs[directory]

    def excludes(self, path: Path) -> bool:
        parts = path.relative_to(self._folder).parts
        if parts[-1] == IGNORE_FILE:
            return True
        for depth in range(1, len(parts) + 1):
            suffix = "/" if depth < len(parts) or path.is_dir() else ""
            if not IGNORE_SCOPES.match_file("/".join(parts[:depth]) + suffix):
                continue
            excluded = False
            for above in range(1, depth):
                if not IGNORE_SCOPES.match_file("/".join(parts[:above]) + "/"):
                    continue
                result = self._spec(self._folder.joinpath(*parts[:above])).check_file(
                    "/".join(parts[above:depth]) + suffix
                )
                if result.include is not None:
                    excluded = result.include
            if excluded:
                return True
        return False


def _walk(directory: Path, ignore: _IgnoreFiles) -> Iterator[Path]:
    for path in sorted(directory.iterdir()):
        if ignore.excludes(path):
            continue
        yield path
        if path.is_dir() and not path.is_symlink():
            yield from _walk(path, ignore)


def _root_ignore(folder: Path) -> PathSpec[Pattern]:
    lines = _lines(folder / ROOT_IGNORE_FILE) if (folder / ROOT_IGNORE_FILE).is_file() else list(DEFAULT_IGNORES)
    return PathSpec.from_lines("gitignore", lines)


def retained_entries(folder: Path) -> list[Path]:
    """Every file, folder and link in the places a task keeps its content, minus what the ignore rules drop."""
    ignore = _IgnoreFiles(folder)
    found = [folder / name for name in TASK_FILES if (folder / name).exists()]
    for name in TASK_FOLDERS:
        top = folder / name
        if top.exists() and not ignore.excludes(top):
            found.append(top)
            found.extend(_walk(top, ignore))
    root_ignore = _root_ignore(folder)
    kept = [
        path
        for path in found
        if not root_ignore.match_file(path.relative_to(folder).as_posix() + ("/" if path.is_dir() else ""))
    ]
    return sorted(kept, key=lambda path: path.relative_to(folder).as_posix())


def left_out(folder: Path) -> list[str]:
    """The entries directly in a task folder that are not part of the task and that its ignore rules don't drop."""
    root_ignore = _root_ignore(folder)
    kept = {*TASK_FILES, *TASK_FOLDERS, ROOT_IGNORE_FILE}
    return sorted(
        entry.name
        for entry in folder.iterdir()
        if entry.name not in kept and not root_ignore.match_file(entry.name + ("/" if entry.is_dir() else ""))
    )


def task_files(folder: Path) -> list[Path]:
    """The files a task is made of, sorted by their path inside the folder."""
    return [path for path in retained_entries(folder) if path.is_file()]


def content_digest(files: Iterable[tuple[str, str]]) -> str:
    """Name a set of files by their paths and the sha256 of each one's bytes."""
    outer = hashlib.sha256()
    for name, digest in sorted(files):
        outer.update(f"{name}\0{digest}\n".encode())
    return DIGEST_PREFIX + outer.hexdigest()


def _digest(read: Callable[[int], bytes]) -> str:
    digest = hashlib.sha256()
    while chunk := read(READ_CHUNK_BYTES):
        digest.update(chunk)
    return digest.hexdigest()


def task_digest(folder: Path) -> str:
    """The digest of a task folder."""
    files: list[tuple[str, str]] = []
    for path in task_files(folder):
        with path.open("rb") as source:
            files.append((path.relative_to(folder).as_posix(), _digest(source.read)))
    return content_digest(files)


def archive_digest(archive: zipfile.ZipFile) -> str:
    """The digest of the files in a packed task, which equals the digest of the folder it was packed from."""
    files: list[tuple[str, str]] = []
    for info in archive.infolist():
        if info.is_dir():
            continue
        with archive.open(info) as source:
            files.append((info.filename, _digest(source.read)))
    return content_digest(files)
