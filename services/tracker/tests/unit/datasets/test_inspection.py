import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from zipfile import ZIP_STORED

import pytest

from tests.unit.datasets.support import zip_bytes
from tracker.clients import archive as archive_client
from tracker.clients.archive import ArchiveReadLimitError, ArchiveReadTimeoutError
from tracker.local.storage import FilesystemObjectStore
from tracker.services.datasets.extract import UnsafeArchiveError
from tracker.services.datasets.inspection import file_preview, files_page, inspect_archive


@pytest.mark.description("Inspecting one small file does not read the large unrelated file in its archive.")
async def test_inspection_reads_only_selected_ranges(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
    content = zip_bytes({"huge.bin": b"x" * (2 * 1024 * 1024), "instruction.md": b"Do this."}, compression=ZIP_STORED)
    await store.put_bytes("task.zip", content)
    ranges: list[tuple[int, int]] = []
    get_range = store.get_range

    async def record_range(key: str, start: int, length: int) -> bytes:
        ranges.append((start, length))

        return await get_range(key, start, length)

    monkeypatch.setattr(store, "get_range", record_range)
    preview = await inspect_archive(store, "task.zip", len(content), partial(file_preview, path="instruction.md"))

    assert preview.content == "Do this."
    assert sum(length for _, length in ranges) < 100_000


@pytest.mark.description("An inspection that exhausts its byte or request budget refuses further storage reads.")
@pytest.mark.parametrize("budget", ["MAX_ARCHIVE_READ_BYTES", "MAX_ARCHIVE_READ_REQUESTS"])
async def test_inspection_enforces_read_budget(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, budget: str) -> None:
    store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
    content = zip_bytes({"instruction.md": b"Do this."})
    await store.put_bytes("task.zip", content)
    monkeypatch.setattr(archive_client, budget, 1)

    with pytest.raises(ArchiveReadLimitError):
        await inspect_archive(store, "task.zip", len(content), partial(files_page, cursor=None, limit=100))


@pytest.mark.description("Inspection refuses unsafe archive paths rather than returning their contents.")
async def test_inspection_rejects_unsafe_paths(tmp_path: Path) -> None:
    store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
    content = zip_bytes({"../outside": b"not a task file"})
    await store.put_bytes("task.zip", content)

    with pytest.raises(UnsafeArchiveError):
        await inspect_archive(store, "task.zip", len(content), partial(files_page, cursor=None, limit=100))


@pytest.mark.description("Cancellation waits for the archive reader to finish its bounded storage reads.")
async def test_cancelled_inspection_finishes_reader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
    content = zip_bytes({"instruction.md": b"Do this."})
    await store.put_bytes("task.zip", content)
    started = asyncio.Event()
    release = asyncio.Event()
    get_range = store.get_range
    active = 0

    async def held_range(key: str, start: int, length: int) -> bytes:
        nonlocal active
        active += 1
        try:
            started.set()
            await release.wait()

            return await get_range(key, start, length)
        finally:
            active -= 1

    monkeypatch.setattr(store, "get_range", held_range)
    inspection = asyncio.create_task(
        inspect_archive(store, "task.zip", len(content), partial(files_page, cursor=None, limit=100))
    )
    await asyncio.wait_for(started.wait(), timeout=5)
    inspection.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(inspection, timeout=5)
    assert active == 0


@pytest.mark.description("Local ZIP workers do not occupy the executor required by filesystem range reads.")
def test_inspection_with_one_filesystem_worker(tmp_path: Path) -> None:
    async def inspect() -> list[str]:
        store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
        content = zip_bytes({"instruction.md": b"Do this."})
        await store.put_bytes("task.zip", content)
        pages = await asyncio.gather(
            *[
                inspect_archive(store, "task.zip", len(content), partial(files_page, cursor=None, limit=100))
                for _ in range(4)
            ]
        )

        return [page.files[0].path for page in pages]

    with asyncio.Runner() as runner:
        runner.get_loop().set_default_executor(ThreadPoolExecutor(max_workers=1))
        paths = runner.run(inspect())

    assert paths == ["instruction.md"] * 4


@pytest.mark.description("A storage timeout finishes cancellation cleanup before the inspection returns.")
async def test_timeout_finishes_storage_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")
    content = zip_bytes({"instruction.md": b"Do this."})
    await store.put_bytes("task.zip", content)
    finished = asyncio.Event()
    timeout = asyncio.timeout

    def immediate_timeout(_delay: float | None) -> asyncio.Timeout:
        return timeout(0)

    async def timed_out_range(_key: str, _start: int, _length: int) -> bytes:
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            finished.set()

        raise AssertionError("The stalled read must time out")

    monkeypatch.setattr(store, "get_range", timed_out_range)
    monkeypatch.setattr(archive_client.asyncio, "timeout", immediate_timeout)

    with pytest.raises(ArchiveReadTimeoutError):
        await inspect_archive(store, "task.zip", len(content), partial(files_page, cursor=None, limit=100))
    assert finished.is_set()
