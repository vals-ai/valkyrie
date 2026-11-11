import zipfile
from collections.abc import Callable
from pathlib import Path

import pytest

from tracker.services.datasets.digest import archive_digest as tracker_archive_digest
from tracker.services.datasets.extract import UnsafeArchiveError, check_archive, extract
from tracker.services.datasets.metric_script import InvalidMetricScriptError, check_metric_script, file_digest
from tracker.services.datasets.source import discover_task_folders
from tracker.services.datasets.validate import validate_task
from tracker.services.datasets.version_ref import validate_tag as tracker_validate_tag
from valkyrie.sdk.task_archive import find_task_folders, pack_task, task_problems
from valkyrie.sdk.metric_script import MetricScriptError, metric_digest, read_metric_script
from valkyrie.sdk.models.datasets import validate_tag as sdk_validate_tag
from valkyrie.sdk.task_content import task_digest

TASK = {
    "task.toml": '[environment]\ndocker_image = "ubuntu:24.04"\n',
    "instruction.md": "Do it.",
    "tests/test.sh": "#!/bin/bash\n",
    "tests/cases/one.txt": "1",
    "environment/data/blob.png": "x" * 100,
}


def write_task(root: Path, name: str) -> Path:
    for relative, text in TASK.items():
        (root / name / relative).parent.mkdir(parents=True, exist_ok=True)
        (root / name / relative).write_text(text)
    (root / name / "tests" / "test.sh").chmod(0o755)
    return root / name


@pytest.mark.description("A packed task passes the trackers checks and extracts file for file.")
def test_a_packed_task_passes_the_trackers_checks_and_extracts_file_for_file(tmp_path: Path) -> None:
    task = write_task(tmp_path / "source", "task-a")
    archive_path = tmp_path / "task-a.zip"
    destination = tmp_path / "extracted"
    pack_task(task, archive_path)

    with zipfile.ZipFile(archive_path) as archive:
        check_archive(archive)
        extract(archive, ["task.toml", "instruction.md", "tests", "environment"], destination)

    extracted = {
        path.relative_to(destination).as_posix(): path.read_text() for path in destination.rglob("*") if path.is_file()
    }
    assert extracted == TASK
    assert validate_task(destination).ok


@pytest.mark.description("The trackers digest of a packed task is the sdks digest of its folder.")
def test_the_trackers_digest_of_a_packed_task_is_the_sdks_digest_of_its_folder(tmp_path: Path) -> None:
    task = write_task(tmp_path / "source", "task-a")
    (task / ".gitignore").write_text("*.log\n")
    (task / "environment" / "build.log").write_text("ignored")
    archive_path = tmp_path / "task-a.zip"
    pack_task(task, archive_path)

    with zipfile.ZipFile(archive_path) as archive:
        assert tracker_archive_digest(archive) == task_digest(task)


@pytest.mark.description("The sdk and the tracker find the same task folders.")
def test_the_sdk_and_the_tracker_find_the_same_task_folders(tmp_path: Path) -> None:
    write_task(tmp_path, "b-task")
    inner = write_task(tmp_path / "group", "a-task")
    write_task(inner / "tests" / "fixtures", "nested")

    assert find_task_folders(tmp_path) == discover_task_folders(tmp_path)


@pytest.mark.description("Every name the tracker refuses is a problem the sdk reports.")
@pytest.mark.parametrize("name", ["a:b.txt", "a\\b.txt", "ok/c:d.txt"])
def test_every_name_the_tracker_refuses_is_a_problem_the_sdk_reports(tmp_path: Path, name: str) -> None:
    task = write_task(tmp_path / "source", "task-a")
    (task / "environment" / name).parent.mkdir(parents=True, exist_ok=True)
    (task / "environment" / name).write_text("x")
    archive_path = tmp_path / "task-a.zip"
    pack_task(task, archive_path)

    with zipfile.ZipFile(archive_path) as archive, pytest.raises(UnsafeArchiveError, match="unsafe path"):
        check_archive(archive)

    assert any(name in problem for problem in task_problems(task))


@pytest.mark.description("The sdk and the tracker accept the same tags.")
@pytest.mark.parametrize(
    "tag", ["stable", "1.0", "v2", "2024-05", "a", "1", "007", "V1", "a_b", "-x", ".x", "", "sha256:abcdef12", "a b"]
)
def test_the_sdk_and_the_tracker_accept_the_same_tags(tag: str) -> None:
    def accepted(validate: Callable[[str], str]) -> bool:
        try:
            validate(tag)
        except ValueError:
            return False
        return True

    assert accepted(sdk_validate_tag) == accepted(tracker_validate_tag)


_HEADER = "# /// script\n# dependencies = [\n#   'numpy',\n# ]\n# ///\n"


@pytest.mark.description("The sdk and the tracker accept the same metric scripts.")
@pytest.mark.parametrize(
    "script",
    [
        b"print('ok')\n",
        (_HEADER + "import numpy\n").encode(),
        b"def broken(:\n",
        b"\xff\xfe",
        b"#" * (256 * 1024 + 1),
        b"# /// script\n# dependencies = [\n# ///\nx = 1\n",
        (_HEADER + "x = 1\n" + _HEADER).encode(),
        b"# /// notes\n# {\n# ///\nx = 1\n",
    ],
    ids=["plain", "header", "syntax", "not-utf8", "too-large", "bad-header", "two-headers", "other-block"],
)
def test_the_sdk_and_the_tracker_accept_the_same_metric_scripts(tmp_path: Path, script: bytes) -> None:
    (tmp_path / "metric.py").write_bytes(script)

    def accepted(check: Callable[[], object]) -> bool:
        try:
            check()
        except (MetricScriptError, InvalidMetricScriptError):
            return False
        return True

    assert accepted(lambda: read_metric_script(tmp_path)) == accepted(lambda: check_metric_script(script))
    assert metric_digest(script) == file_digest(script)
