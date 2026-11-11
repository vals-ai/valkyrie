import asyncio
import os
import tempfile
import zipfile
from collections.abc import AsyncGenerator, Collection, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from typing import Protocol
from uuid import UUID

from tracker.services.datasets.extract import UnsafeArchiveError, check_archive, extract
from tracker.services.datasets.folder import (
    COMPOSE_FILE,
    CONFIG_FILE,
    DOCKERFILE,
    ENVIRONMENT_DIR,
    INSTRUCTION_FILE,
    REQUIRED_FILES,
    TEST_SCRIPT,
)
from tracker.services.datasets.steps import STEP_TESTS, STEPS_DIR
from tracker.runtime.artifacts import dataset_blob_key
from tracker.runtime.storage import ObjectStore

TASK_FILES = (
    *REQUIRED_FILES,
    f"{ENVIRONMENT_DIR}/{DOCKERFILE}",
    f"{ENVIRONMENT_DIR}/{COMPOSE_FILE}",
    f"tests/{DOCKERFILE}",
    f"tests/{COMPOSE_FILE}",
    f"{STEPS_DIR}/*/{INSTRUCTION_FILE}",
    f"{STEPS_DIR}/*/{TEST_SCRIPT}",
    f"{STEPS_DIR}/*/{STEP_TESTS}/{DOCKERFILE}",
    f"{STEPS_DIR}/*/{STEP_TESTS}/{COMPOSE_FILE}",
)


class TaskSource(Protocol):
    async def task_ids(self) -> list[str]: ...

    def open_task(self, task_id: str, parts: Collection[str] = ()) -> AbstractAsyncContextManager[Path]: ...


def discover_task_folders(root: Path) -> dict[str, Path]:
    folders: dict[str, Path] = {}
    for directory, subdirectories, files in os.walk(root):
        path = Path(directory)
        if path == root or CONFIG_FILE not in files:
            continue
        subdirectories.clear()
        if path.name in folders:
            raise ValueError(f"Two tasks are named {path.name!r}: {folders[path.name]} and {path}")
        folders[path.name] = path
    return dict(sorted(folders.items()))


class LocalFolderSource:
    def __init__(self, root: Path) -> None:
        self._root = root

    async def task_ids(self) -> list[str]:
        return list(await asyncio.to_thread(self._folders))

    @asynccontextmanager
    async def open_task(self, task_id: str, parts: Collection[str] = ()) -> AsyncGenerator[Path]:
        folders = await asyncio.to_thread(self._folders)
        if task_id not in folders:
            raise FileNotFoundError(f"Task {task_id!r} is not under {self._root}")
        yield folders[task_id]

    def _folders(self) -> dict[str, Path]:
        if not self._root.is_dir():
            raise FileNotFoundError(f"{self._root} is not a folder")
        return discover_task_folders(self._root)


class ArchiveSource:
    def __init__(self, objects: ObjectStore, dataset_id: UUID, tasks: Mapping[str, str]) -> None:
        self._objects = objects
        self._dataset_id = str(dataset_id)
        self._tasks = dict(tasks)

    async def task_ids(self) -> list[str]:
        return sorted(self._tasks)

    @asynccontextmanager
    async def open_task(self, task_id: str, parts: Collection[str] = ()) -> AsyncGenerator[Path]:
        if task_id not in self._tasks:
            raise FileNotFoundError(f"The dataset version has no task {task_id!r}")
        key = dataset_blob_key(self._dataset_id, self._tasks[task_id])
        with tempfile.TemporaryDirectory(prefix="task-") as scratch:
            archive_path = Path(scratch) / "archive.zip"
            task_dir = Path(scratch) / "tasks" / task_id
            task_dir.mkdir(parents=True)
            await self._download(key, archive_path)
            await asyncio.to_thread(self._unpack, archive_path, task_dir, [*TASK_FILES, *parts])
            archive_path.unlink()
            yield task_dir

    async def _download(self, key: str, destination: Path) -> None:
        sink = await asyncio.to_thread(destination.open, "xb")
        try:
            async for chunk in self._objects.get_stream(key):
                await asyncio.to_thread(sink.write, chunk)
        finally:
            await asyncio.to_thread(sink.close)

    @staticmethod
    def _unpack(archive_path: Path, task_dir: Path, parts: Collection[str]) -> None:
        try:
            with zipfile.ZipFile(archive_path) as archive:
                check_archive(archive)
                extract(archive, parts, task_dir)
        except zipfile.BadZipFile as error:
            raise UnsafeArchiveError(f"The task archive is not a readable zip file: {error}") from error
