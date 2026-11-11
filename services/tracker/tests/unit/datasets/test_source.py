from pathlib import Path
from uuid import uuid4

import pytest

from tests.unit.datasets.support import TASK, store_archive, zip_bytes
from tracker.services.datasets.extract import UnsafeArchiveError
from tracker.services.datasets.source import ArchiveSource, LocalFolderSource, discover_task_folders
from tracker.local.storage import FilesystemObjectStore
from tracker.runtime.artifacts import dataset_blob_key


def write_task(root: Path, name: str, files: dict[str, str] | None = None) -> Path:
    task = root / name
    for relative, text in {"task.toml": "", "instruction.md": "Do it.", **(files or {})}.items():
        (task / relative).parent.mkdir(parents=True, exist_ok=True)
        (task / relative).write_text(text)
    return task


@pytest.mark.description("Finding the tasks under a dataset folder.")
class TestDiscoverTaskFolders:
    @pytest.mark.description("Tasks are found at any depth and keyed by their folder name.")
    def test_tasks_are_found_at_any_depth_and_keyed_by_their_folder_name(self, tmp_path: Path) -> None:
        write_task(tmp_path, "b-task")
        write_task(tmp_path / "group", "a-task")

        assert list(discover_task_folders(tmp_path)) == ["a-task", "b-task"]

    @pytest.mark.description("Nothing inside a task is another task.")
    def test_nothing_inside_a_task_is_another_task(self, tmp_path: Path) -> None:
        task = write_task(tmp_path, "outer")
        write_task(task / "tests" / "fixtures", "inner")

        assert list(discover_task_folders(tmp_path)) == ["outer"]

    @pytest.mark.description("The root itself is not a task.")
    def test_the_root_itself_is_not_a_task(self, tmp_path: Path) -> None:
        write_task(tmp_path.parent, tmp_path.name)

        assert discover_task_folders(tmp_path) == {}

    @pytest.mark.description("Two tasks with one name are refused.")
    def test_two_tasks_with_one_name_are_refused(self, tmp_path: Path) -> None:
        write_task(tmp_path / "one", "same")
        write_task(tmp_path / "two", "same")

        with pytest.raises(ValueError, match="Two tasks are named 'same'"):
            discover_task_folders(tmp_path)


@pytest.mark.description("Tasks read in place from a folder.")
class TestLocalFolderSource:
    @pytest.mark.description("Task ids and the folder itself are returned without a copy.")
    async def test_task_ids_and_the_folder_itself_are_returned_without_a_copy(self, tmp_path: Path) -> None:
        folder = write_task(tmp_path, "hello")
        source = LocalFolderSource(tmp_path)

        assert await source.task_ids() == ["hello"]
        async with source.open_task("hello", ["tests"]) as opened:
            assert opened == folder

    @pytest.mark.description("An unknown task or folder is not found.")
    async def test_an_unknown_task_or_folder_is_not_found(self, tmp_path: Path) -> None:
        write_task(tmp_path, "hello")

        with pytest.raises(FileNotFoundError, match="'nope'"):
            async with LocalFolderSource(tmp_path).open_task("nope"):
                pass
        with pytest.raises(FileNotFoundError, match="is not a folder"):
            await LocalFolderSource(tmp_path / "missing").task_ids()


@pytest.fixture
def store(tmp_path: Path) -> FilesystemObjectStore:
    return FilesystemObjectStore(tmp_path / "objects", tmp_path / "staging")


@pytest.mark.description("Tasks read from a dataset's archives in storage.")
class TestArchiveSource:
    @pytest.mark.description("Task ids are the versions tasks in order.")
    async def test_task_ids_are_the_versions_tasks_in_order(self, store: FilesystemObjectStore) -> None:
        digest = await store_archive(store, uuid4(), zip_bytes(TASK))

        source = ArchiveSource(store, uuid4(), {"task-b": digest, "task-a": digest})

        assert await source.task_ids() == ["task-a", "task-b"]

    @pytest.mark.description("A task opens with its light files and only the parts asked for.")
    async def test_a_task_opens_with_its_light_files_and_only_the_parts_asked_for(
        self, store: FilesystemObjectStore
    ) -> None:
        dataset_id = uuid4()
        digest = await store_archive(
            store,
            dataset_id,
            zip_bytes({**TASK, "tests/Dockerfile": b"FROM grader", "tests/docker-compose.yaml": b"services: {}"}),
        )
        source = ArchiveSource(store, dataset_id, {"task-1": digest})

        async with source.open_task("task-1") as light:
            assert sorted(path.relative_to(light).as_posix() for path in light.rglob("*") if path.is_file()) == [
                "environment/Dockerfile",
                "instruction.md",
                "task.toml",
                "tests/Dockerfile",
                "tests/docker-compose.yaml",
                "tests/test.sh",
            ]
        async with source.open_task("task-1", ["tests"]) as graded:
            assert (graded / "tests" / "cases" / "one.txt").read_bytes() == b"1"
            assert not (graded / "environment" / "data").exists()
            assert not (graded / "solution").exists()

    @pytest.mark.description("The archive is gone while the task is in use and everything is gone after.")
    async def test_the_archive_is_gone_while_the_task_is_in_use_and_everything_is_gone_after(
        self, store: FilesystemObjectStore
    ) -> None:
        dataset_id = uuid4()
        digest = await store_archive(store, dataset_id, zip_bytes(TASK))

        async with ArchiveSource(store, dataset_id, {"task-1": digest}).open_task("task-1") as task_dir:
            scratch = task_dir.parent.parent
            assert task_dir.name == "task-1"
            assert sorted(path.name for path in scratch.iterdir()) == ["tasks"]

        assert not scratch.exists()

    @pytest.mark.description("The scratch space is removed when the phase fails.")
    async def test_the_scratch_space_is_removed_when_the_phase_fails(self, store: FilesystemObjectStore) -> None:
        dataset_id = uuid4()
        digest = await store_archive(store, dataset_id, zip_bytes(TASK))
        scratch: Path | None = None

        with pytest.raises(RuntimeError, match="phase failed"):
            async with ArchiveSource(store, dataset_id, {"task-1": digest}).open_task("task-1") as task_dir:
                scratch = task_dir.parent.parent
                raise RuntimeError("phase failed")

        assert scratch is not None
        assert not scratch.exists()

    @pytest.mark.description("A task outside the version is not found.")
    async def test_a_task_outside_the_version_is_not_found(self, store: FilesystemObjectStore) -> None:
        digest = await store_archive(store, uuid4(), zip_bytes(TASK))

        with pytest.raises(FileNotFoundError):
            async with ArchiveSource(store, uuid4(), {"task-1": digest}).open_task("task-2"):
                pass

    @pytest.mark.description("An object that is not a zip is refused.")
    async def test_an_object_that_is_not_a_zip_is_refused(self, store: FilesystemObjectStore) -> None:
        dataset_id = uuid4()
        digest = "sha256:" + "0" * 64
        await store.put_bytes(dataset_blob_key(str(dataset_id), digest), b"not a zip" * 10)

        with pytest.raises(UnsafeArchiveError, match="not a readable zip"):
            async with ArchiveSource(store, dataset_id, {"task-1": digest}).open_task("task-1"):
                pass

    @pytest.mark.description("An archive with an unsafe path is refused before anything is written.")
    @pytest.mark.parametrize("name", ["../escape.txt", "/abs.txt"])
    async def test_an_archive_with_an_unsafe_path_is_refused_before_anything_is_written(
        self, store: FilesystemObjectStore, name: str
    ) -> None:
        dataset_id = uuid4()
        digest = "sha256:" + "1" * 64
        await store.put_bytes(dataset_blob_key(str(dataset_id), digest), zip_bytes({**TASK, name: b"x"}))

        with pytest.raises(UnsafeArchiveError, match="unsafe path"):
            async with ArchiveSource(store, dataset_id, {"task-1": digest}).open_task("task-1"):
                pass
