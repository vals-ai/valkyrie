import io
import os
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from valkyrie.sdk.task_archive import TaskFolderError, find_task_folders, pack_task, task_problems

TASK_FILES = {"task.toml": "[agent]\ntimeout_sec = 60\n", "instruction.md": "Do it.", "tests/test.sh": "#!/bin/bash\n"}


def write_task(root: Path, name: str, extra: dict[str, str] | None = None) -> Path:
    task = root / name
    for relative, text in {**TASK_FILES, **(extra or {})}.items():
        (task / relative).parent.mkdir(parents=True, exist_ok=True)
        (task / relative).write_text(text)
    return task


def packed(folder: Path, tmp_path: Path, name: str = "task.zip") -> zipfile.ZipFile:
    archive = tmp_path / name
    pack_task(folder, archive)
    return zipfile.ZipFile(io.BytesIO(archive.read_bytes()))


@pytest.mark.description("Finding the tasks under a folder.")
class TestFindTaskFolders:
    @pytest.mark.description("Tasks are found at any depth by folder name and nothing inside one is another.")
    def test_tasks_are_found_at_any_depth_by_folder_name_and_nothing_inside_one_is_another(
        self, tmp_path: Path
    ) -> None:
        write_task(tmp_path, "b-task")
        outer = write_task(tmp_path / "group", "a-task")
        write_task(outer / "tests" / "fixtures", "inner")

        assert list(find_task_folders(tmp_path)) == ["a-task", "b-task"]

    @pytest.mark.description("Version control folders are not searched.")
    def test_version_control_folders_are_not_searched(self, tmp_path: Path) -> None:
        write_task(tmp_path, "real")
        write_task(tmp_path / ".git", "ignored")

        assert list(find_task_folders(tmp_path)) == ["real"]

    @pytest.mark.description("A folder without tasks below it is refused.")
    @pytest.mark.parametrize("layout", ["empty", "missing", "task"])
    def test_a_folder_without_tasks_below_it_is_refused(self, tmp_path: Path, layout: str) -> None:
        root = tmp_path / "root"
        if layout == "task":
            write_task(tmp_path, "root")
        elif layout == "empty":
            root.mkdir()

        with pytest.raises(TaskFolderError, match="is not a folder|No task folders|is a task folder"):
            find_task_folders(root)

    @pytest.mark.description("Two tasks with one name are refused.")
    def test_two_tasks_with_one_name_are_refused(self, tmp_path: Path) -> None:
        write_task(tmp_path / "one", "same")
        write_task(tmp_path / "two", "same")

        with pytest.raises(TaskFolderError, match="Two tasks are named 'same'"):
            find_task_folders(tmp_path)


@pytest.mark.description("What stops a folder from being packed.")
class TestTaskProblems:
    @pytest.mark.description("A complete task has none.")
    def test_a_complete_task_has_none(self, tmp_path: Path) -> None:
        assert task_problems(write_task(tmp_path, "good-task")) == []

    @pytest.mark.description("Missing files are named.")
    def test_missing_files_are_named(self, tmp_path: Path) -> None:
        task = write_task(tmp_path, "task")
        (task / "instruction.md").unlink()
        (task / "tests" / "test.sh").unlink()

        assert task_problems(task) == ["instruction.md is missing", "tests/test.sh is missing"]

    @pytest.mark.description("A task with steps needs no instruction of its own but one for each step.")
    def test_a_task_with_steps_needs_no_instruction_of_its_own_but_one_for_each_step(self, tmp_path: Path) -> None:
        config = "[[steps]]\nname = 'first'\n[[steps]]\nname = 'second'\n"
        task = write_task(tmp_path, "stepped", {"task.toml": config, "steps/first/instruction.md": "One."})
        (task / "instruction.md").unlink()

        assert task_problems(task) == ["steps/second/instruction.md is missing"]

    @pytest.mark.description("A step may share the tasks test script.")
    def test_a_step_may_share_the_tasks_test_script(self, tmp_path: Path) -> None:
        extra = {"task.toml": "[[steps]]\nname = 'first'\n", "steps/first/instruction.md": "One."}

        assert task_problems(write_task(tmp_path, "task", extra)) == []

    @pytest.mark.description("A step may bring its own test script.")
    def test_a_step_may_bring_its_own_test_script(self, tmp_path: Path) -> None:
        extra = {
            "task.toml": "[[steps]]\nname = 'first'\n",
            "steps/first/instruction.md": "One.",
            "steps/first/tests/test.sh": "#!/bin/bash\n",
        }
        task = write_task(tmp_path, "task", extra)
        (task / "tests" / "test.sh").unlink()

        assert task_problems(task) == []

    @pytest.mark.description("A step without any test script is reported.")
    def test_a_step_without_any_test_script_is_reported(self, tmp_path: Path) -> None:
        extra = {"task.toml": "[[steps]]\nname = 'first'\n", "steps/first/instruction.md": "One."}
        task = write_task(tmp_path, "task", extra)
        (task / "tests" / "test.sh").unlink()

        assert task_problems(task) == ["step 'first' has no tests/test.sh of its own or in the task's tests"]

    @pytest.mark.description("A step name that is not a folder name is reported.")
    def test_a_step_name_that_is_not_a_folder_name_is_reported(self, tmp_path: Path) -> None:
        task = write_task(tmp_path, "task", {"task.toml": "[[steps]]\nname = '../escape'\n"})

        (problem,) = task_problems(task)

        assert "'../escape' must be a folder name" in problem

    @pytest.mark.description("A config that is not toml is reported.")
    def test_a_config_that_is_not_toml_is_reported(self, tmp_path: Path) -> None:
        task = write_task(tmp_path, "task", {"task.toml": "this is = = not toml"})

        (problem,) = task_problems(task)

        assert problem.startswith("task.toml is not valid TOML")

    @pytest.mark.description("A folder name that cannot be a task id is reported.")
    def test_a_folder_name_that_cannot_be_a_task_id_is_reported(self, tmp_path: Path) -> None:
        (problem,) = task_problems(write_task(tmp_path, "has space"))

        assert "has space" in problem

    @pytest.mark.description("Links are refused.")
    def test_links_are_refused(self, tmp_path: Path) -> None:
        task = write_task(tmp_path, "task")
        (task / "tests" / "link").symlink_to("/etc/passwd")

        assert task_problems(task) == ["tests/link is a link or special file"]


@pytest.mark.description("The zip a task is stored as.")
class TestPackTask:
    @pytest.mark.description("The same files pack to the same bytes whatever their dates.")
    def test_the_same_files_pack_to_the_same_bytes_whatever_their_dates(self, tmp_path: Path) -> None:
        first = write_task(tmp_path / "one", "task", {"environment/a.txt": "a", "environment/b.txt": "b"})
        second = write_task(tmp_path / "two", "task", {"environment/b.txt": "b", "environment/a.txt": "a"})
        os.utime(second / "environment" / "a.txt", (1_000_000_000, 1_000_000_000))

        pack_task(first, tmp_path / "first.zip")
        pack_task(second, tmp_path / "second.zip")

        assert (tmp_path / "first.zip").read_bytes() == (tmp_path / "second.zip").read_bytes()

    @pytest.mark.description("Entries are sorted dated the same and leave out caches.")
    def test_entries_are_sorted_dated_the_same_and_leave_out_caches(self, tmp_path: Path) -> None:
        extra = {
            "environment/b.txt": "b",
            "environment/a.txt": "a",
            "environment/.DS_Store": "x",
            "environment/__pycache__/m.pyc": "x",
        }
        task = write_task(tmp_path, "task", extra)

        archive = packed(task, tmp_path)

        names = [entry.filename for entry in archive.infolist()]
        assert names == ["environment/a.txt", "environment/b.txt", "instruction.md", "task.toml", "tests/test.sh"]
        assert {entry.date_time for entry in archive.infolist()} == {(1980, 1, 1, 0, 0, 0)}

    @pytest.mark.description("Executable files stay executable.")
    def test_executable_files_stay_executable(self, tmp_path: Path) -> None:
        task = write_task(tmp_path, "task")
        (task / "tests" / "test.sh").chmod(0o755)

        archive = packed(task, tmp_path)

        modes = {entry.filename: stat.S_IMODE(entry.external_attr >> 16) for entry in archive.infolist()}
        assert modes["tests/test.sh"] == 0o755
        assert modes["task.toml"] == 0o644

    @pytest.mark.description("Files that are already compressed are stored and text is deflated.")
    def test_files_that_are_already_compressed_are_stored_and_text_is_deflated(self, tmp_path: Path) -> None:
        task = write_task(tmp_path, "task", {"environment/image.png": "x" * 1000})

        archive = packed(task, tmp_path)

        compression = {entry.filename: entry.compress_type for entry in archive.infolist()}
        assert compression["environment/image.png"] == zipfile.ZIP_STORED
        assert compression["instruction.md"] == zipfile.ZIP_DEFLATED
        assert archive.read("environment/image.png") == b"x" * 1000


@pytest.mark.description("Folders the SDK must not pack because they would fail later, or be packed incomplete.")
class TestWhatTheTrackerWouldRefuse:
    @pytest.mark.description("Names the tracker refuses are problems.")
    @pytest.mark.parametrize("name", ["a:b.txt", "a\\b.txt"])
    def test_names_the_tracker_refuses_are_problems(self, tmp_path: Path, name: str) -> None:
        task = write_task(tmp_path, "task", {f"environment/data/{name}": "x"})

        (problem,) = task_problems(task)

        assert name in problem

    @pytest.mark.description("A folder that cannot be read is an error not a gap.")
    @pytest.mark.skipif(os.geteuid() == 0, reason="root reads every folder")
    def test_a_folder_that_cannot_be_read_is_an_error_not_a_gap(self, tmp_path: Path) -> None:
        write_task(tmp_path / "group", "task")
        task = write_task(tmp_path, "other", {"environment/data/big.txt": "x"})
        (tmp_path / "group").chmod(0)
        (task / "environment" / "data").chmod(0)
        try:
            with pytest.raises(PermissionError):
                find_task_folders(tmp_path)
            with pytest.raises(PermissionError):
                task_problems(task)
        finally:
            (tmp_path / "group").chmod(0o755)
            (task / "environment" / "data").chmod(0o755)

    @pytest.mark.description("A config with non ascii text is read as utf 8.")
    def test_a_config_with_non_ascii_text_is_read_as_utf_8(self, tmp_path: Path) -> None:
        task = write_task(tmp_path, "task", {"task.toml": "# café\n[agent]\ntimeout_sec = 60\n"})
        script = (
            "import sys; from pathlib import Path; from valkyrie.sdk.task_archive import task_problems; "
            "print(task_problems(Path(sys.argv[1])))"
        )
        environment = {**os.environ, "LC_ALL": "C", "PYTHONUTF8": "0", "PYTHONCOERCECLOCALE": "0"}

        result = subprocess.run(
            [sys.executable, "-X", "utf8=0", "-c", script, str(task)],
            capture_output=True,
            text=True,
            env=environment,
            check=True,
        )

        assert result.stdout.strip() == "[]"
