"""Check a task folder and pack it into the zip archive a dataset stores."""

import os
import re
import shutil
import stat
import tomllib
import zipfile
from pathlib import Path
from typing import Any, NoReturn, TypeGuard

from valkyrie.sdk.models.datasets import validate_task_id
from valkyrie.sdk.task_content import retained_entries, task_files

CONFIG_FILE = "task.toml"
INSTRUCTION_FILE = "instruction.md"
TEST_SCRIPT = "tests/test.sh"
STEPS_DIR = "steps"
STEP_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
IGNORED_NAMES = frozenset({".git", ".DS_Store", "__pycache__"})
STORED_SUFFIXES = frozenset(
    {".7z", ".bin", ".bz2", ".gz", ".jpeg", ".jpg", ".mp4", ".parquet", ".png", ".pt", ".webp", ".xz", ".zip", ".zst"}
)
FIXED_TIME = (1980, 1, 1, 0, 0, 0)
COPY_CHUNK_BYTES = 1024 * 1024


class TaskFolderError(ValueError):
    pass


def _raise(error: OSError) -> NoReturn:
    raise error


def find_task_folders(root: Path) -> dict[str, Path]:
    """Find the task folders under root at any depth, by folder name; nothing inside a task is another task."""
    if not root.is_dir():
        raise TaskFolderError(f"{root} is not a folder")
    folders: dict[str, Path] = {}
    for directory, subdirectories, files in os.walk(root, onerror=_raise):
        subdirectories[:] = [name for name in subdirectories if name not in IGNORED_NAMES]
        path = Path(directory)
        if CONFIG_FILE not in files:
            continue
        if path == root:
            raise TaskFolderError(f"{root} is a task folder; pass the folder that holds the tasks")
        subdirectories.clear()
        if path.name in folders:
            raise TaskFolderError(f"Two tasks are named {path.name!r}: {folders[path.name]} and {path}")
        folders[path.name] = path
    if not folders:
        raise TaskFolderError(f"No task folders (folders holding {CONFIG_FILE}) found under {root}")
    return dict(sorted(folders.items()))


def task_problems(folder: Path) -> list[str]:
    """What stops a task folder from being packed, or an empty list."""
    problems: list[str] = []
    try:
        validate_task_id(folder.name)
    except ValueError as error:
        problems.append(f"the folder name {folder.name!r} {error}")
    steps: list[str] = []
    config = folder / CONFIG_FILE
    if config.is_file():
        try:
            steps = _step_names(tomllib.loads(config.read_text(encoding="utf-8")))
        except (tomllib.TOMLDecodeError, UnicodeDecodeError) as error:
            problems.append(f"{CONFIG_FILE} is not valid TOML: {error}")
    else:
        problems.append(f"{CONFIG_FILE} is missing")
    if steps:
        problems.extend(_step_problems(folder, steps))
    else:
        problems.extend(
            f"{name} is missing" for name in (INSTRUCTION_FILE, TEST_SCRIPT) if not (folder / name).is_file()
        )
    for path in retained_entries(folder):
        relative = path.relative_to(folder).as_posix()
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            problems.append(f"{relative} is a link or special file")
        elif ":" in relative or "\\" in relative:
            problems.append(f"{relative} has a ':' or a backslash in its name, which the Tracker refuses")
    return problems


def _is_list(value: object) -> TypeGuard[list[Any]]:
    return isinstance(value, list)


def _is_table(value: object) -> TypeGuard[dict[str, Any]]:
    return isinstance(value, dict)


def _step_names(raw: dict[str, Any]) -> list[str]:
    steps = raw.get("steps")
    if not _is_list(steps):
        return []
    names = [step.get("name") if _is_table(step) else None for step in steps]
    return [name if isinstance(name, str) else "" for name in names]


def _step_problems(folder: Path, steps: list[str]) -> list[str]:
    problems: list[str] = []
    for name in steps:
        if not STEP_NAME.fullmatch(name):
            problems.append(f"the step name {name!r} must be a folder name of letters, digits, '_', '.' and '-'")
            continue
        if not (folder / STEPS_DIR / name / INSTRUCTION_FILE).is_file():
            problems.append(f"{STEPS_DIR}/{name}/{INSTRUCTION_FILE} is missing")
        if not (folder / STEPS_DIR / name / TEST_SCRIPT).is_file() and not (folder / TEST_SCRIPT).is_file():
            problems.append(f"step {name!r} has no {TEST_SCRIPT} of its own or in the task's tests")
    return problems


def pack_task(folder: Path, destination: Path) -> None:
    """Write the task's files as a zip that is the same bytes for the same files."""
    with zipfile.ZipFile(destination, "w") as archive:
        for path in task_files(folder):
            status = path.stat()
            info = zipfile.ZipInfo(path.relative_to(folder).as_posix(), FIXED_TIME)
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | (0o755 if status.st_mode & stat.S_IXUSR else 0o644)) << 16
            info.compress_type = zipfile.ZIP_STORED if path.suffix.lower() in STORED_SUFFIXES else zipfile.ZIP_DEFLATED
            with (
                path.open("rb") as source,
                archive.open(info, "w", force_zip64=status.st_size >= zipfile.ZIP64_LIMIT) as sink,
            ):
                shutil.copyfileobj(source, sink, COPY_CHUNK_BYTES)
