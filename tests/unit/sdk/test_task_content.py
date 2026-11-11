import io
import os
import zipfile
from pathlib import Path

import pytest

from valkyrie.sdk.task_archive import pack_task
from valkyrie.sdk.task_content import archive_digest, left_out, task_digest, task_files

BASE = {"task.toml": "[agent]\ntimeout_sec = 60\n", "instruction.md": "Do it.", "tests/test.sh": "#!/bin/bash\n"}

FIXTURES: dict[str, tuple[dict[str, str], str]] = {
    "minimal": ({}, "sha256:f5c89499ef91929ea82d97ea86827fb61aba6d66bef9053d5f76728d1dc05bcd"),
    "full layout": (
        {
            "environment/Dockerfile": "FROM ubuntu:24.04\n",
            "README.md": "# Task\n",
            "trajectory.json": "{}",
            "solution/solve.sh": "#!/bin/bash\n",
        },
        "sha256:da73b2153aec2d5dfc3bf66f40c4f99d0afc08e6a7eb707f03ebbb21f17949b6",
    ),
    "default ignores": (
        {
            "environment/keep.py": "x = 1\n",
            "environment/__pycache__/m.pyc": "bytes",
            "environment/.DS_Store": "junk",
            "environment/a.swp": "swap",
            "environment/b~": "backup",
        },
        "sha256:28b55e1760600ead913d109101c4bae6a87581afb859a77b3052ec17e716c715",
    ),
    "gitignore replaces defaults": (
        {
            ".gitignore": "*.log\nbuild/\n",
            "environment/x.log": "log",
            "environment/build/out.bin": "bin",
            "environment/keep.txt": "keep",
            "environment/__pycache__/m.pyc": "kept because defaults are off",
        },
        "sha256:6a0f28fd2e2ae8a446d6a6da2d494920b8dda8e1b7d3bd877e76fae6ec1cdcff",
    ),
    "ignore files inside tests": (
        {
            "tests/.harborignore": "fixtures/\n*.tmp\n",
            "tests/fixtures/a.json": "{}",
            "tests/b.tmp": "tmp",
            "tests/c.py": "pass\n",
            "tests/sub/.harborignore": "!keep.tmp\n",
            "tests/sub/keep.tmp": "kept",
            "tests/sub/drop.tmp": "dropped",
        },
        "sha256:bce2a6d8acca761fe9004bb09f55cf9a15285480781fff5ba2c80451aaba1d6e",
    ),
    "steps": (
        {
            "task.toml": "[[steps]]\nname = 'first'\n",
            "steps/first/instruction.md": "One.",
            "steps/first/tests/test.sh": "#!/bin/bash\n",
            "steps/first/tests/.harborignore": "*.skip\n",
            "steps/first/tests/a.skip": "skipped",
            "steps/first/solution/solve.sh": "#!/bin/bash\n",
            "steps/first/workdir/f.txt": "work",
        },
        "sha256:545c96c8d73370496e0af98f266e0a1b0f4f393773725e5d570e2a7bb9a1fb00",
    ),
    "stray files": (
        {"notes.txt": "not part of the task", "docs/readme.txt": "not part of the task"},
        "sha256:f5c89499ef91929ea82d97ea86827fb61aba6d66bef9053d5f76728d1dc05bcd",
    ),
}


def write_task(root: Path, name: str, extra: dict[str, str] | None = None) -> Path:
    task = root / name
    for relative, text in {**BASE, **(extra or {})}.items():
        (task / relative).parent.mkdir(parents=True, exist_ok=True)
        (task / relative).write_text(text)
    return task


@pytest.mark.description("The digest of a task folder.")
class TestTaskDigest:
    @pytest.mark.description("A task folder has its reference digest.")
    @pytest.mark.parametrize("name", FIXTURES)
    def test_a_task_folder_has_its_reference_digest(self, tmp_path: Path, name: str) -> None:
        extra, expected = FIXTURES[name]

        assert task_digest(write_task(tmp_path, "task", extra)) == expected

    @pytest.mark.description("Empty folders are not part of the digest.")
    def test_empty_folders_are_not_part_of_the_digest(self, tmp_path: Path) -> None:
        task = write_task(tmp_path, "task")
        (task / "environment" / "empty").mkdir(parents=True)

        assert task_digest(task) == FIXTURES["minimal"][1]

    @pytest.mark.description("A changed file changes the digest.")
    def test_a_changed_file_changes_the_digest(self, tmp_path: Path) -> None:
        first = write_task(tmp_path / "one", "task")
        second = write_task(tmp_path / "two", "task", {"instruction.md": "Do it differently."})

        assert task_digest(first) != task_digest(second)

    @pytest.mark.description("The order files were written and their dates do not matter.")
    def test_the_order_files_were_written_and_their_dates_do_not_matter(self, tmp_path: Path) -> None:
        first = write_task(tmp_path / "one", "task", {"environment/a.txt": "a", "environment/b.txt": "b"})
        second = write_task(tmp_path / "two", "task", {"environment/b.txt": "b", "environment/a.txt": "a"})
        os.utime(second / "environment" / "a.txt", (1_000_000_000, 1_000_000_000))

        assert task_digest(first) == task_digest(second)


@pytest.mark.description("The files a task is made of.")
class TestTaskFiles:
    @pytest.mark.description("Only the places a task keeps its content are collected.")
    def test_only_the_places_a_task_keeps_its_content_are_collected(self, tmp_path: Path) -> None:
        extra, _ = FIXTURES["stray files"]
        task = write_task(tmp_path, "task", extra)

        names = [path.relative_to(task).as_posix() for path in task_files(task)]

        assert names == ["instruction.md", "task.toml", "tests/test.sh"]

    @pytest.mark.description("An ignore file inside tests drops files and a deeper one can bring them back.")
    def test_an_ignore_file_inside_tests_drops_files_and_a_deeper_one_can_bring_them_back(self, tmp_path: Path) -> None:
        extra, _ = FIXTURES["ignore files inside tests"]
        task = write_task(tmp_path, "task", extra)

        names = [path.relative_to(task).as_posix() for path in task_files(task)]

        assert names == ["instruction.md", "task.toml", "tests/c.py", "tests/sub/keep.tmp", "tests/test.sh"]


@pytest.mark.description("The digest of a packed task.")
class TestArchiveDigest:
    @pytest.mark.description("A packed task has the digest of the folder it came from.")
    @pytest.mark.parametrize("name", FIXTURES)
    def test_a_packed_task_has_the_digest_of_the_folder_it_came_from(self, tmp_path: Path, name: str) -> None:
        extra, expected = FIXTURES[name]
        archive_path = tmp_path / "task.zip"
        pack_task(write_task(tmp_path, "task", extra), archive_path)

        with zipfile.ZipFile(archive_path) as archive:
            assert archive_digest(archive) == expected

    @pytest.mark.description("Folder entries are not part of the digest.")
    def test_folder_entries_are_not_part_of_the_digest(self) -> None:
        with_folder, without_folder = io.BytesIO(), io.BytesIO()
        with zipfile.ZipFile(with_folder, "w") as archive:
            archive.writestr("environment/", b"")
            archive.writestr("a.txt", b"a")
        with zipfile.ZipFile(without_folder, "w") as archive:
            archive.writestr("a.txt", b"a")

        with zipfile.ZipFile(with_folder) as first, zipfile.ZipFile(without_folder) as second:
            assert archive_digest(first) == archive_digest(second)


@pytest.mark.description("The top-level entries a task folder holds that are not part of the task.")
class TestLeftOut:
    @pytest.mark.description("Entries outside the task are listed unless the ignore rules drop them.")
    def test_entries_outside_the_task_are_listed_unless_the_ignore_rules_drop_them(self, tmp_path: Path) -> None:
        folder = write_task(
            tmp_path, "task", {"data/set.csv": "1", "notes.txt": "n", "README.md": "# Task", ".DS_Store": "junk"}
        )

        assert left_out(folder) == ["data", "notes.txt"]

    @pytest.mark.description("A task gitignore decides what is dropped instead of the defaults.")
    def test_a_task_gitignore_decides_what_is_dropped_instead_of_the_defaults(self, tmp_path: Path) -> None:
        folder = write_task(
            tmp_path, "task", {".gitignore": "*.log\n", "run.log": "x", ".DS_Store": "junk", "notes.txt": "n"}
        )

        assert left_out(folder) == [".DS_Store", "notes.txt"]

    @pytest.mark.description("A folder of only task content leaves nothing out.")
    def test_a_folder_of_only_task_content_leaves_nothing_out(self, tmp_path: Path) -> None:
        assert left_out(write_task(tmp_path, "task", {"environment/Dockerfile": "FROM ubuntu"})) == []
