import json
from pathlib import Path

import httpx
import pytest
from click.testing import CliRunner, Result

from valkyrie.cli.main import cli

from tests.unit.cli.conftest import MockTracker

_URL = "/v1/benchmarks/swe-bench/datasets/verified"
_TASK = {"task_id": "django-11099", "digest": "sha256:" + "ab" * 32, "size": 1234}
_OTHER = {"task_id": "astropy-12907", "digest": "sha256:" + "cd" * 32, "size": 56}
_VERSION = {
    "benchmark": "swe-bench",
    "dataset": "verified",
    "revision": 2,
    "digest": "sha256:" + "ef" * 32,
    "task_count": 2,
    "tags": ["latest"],
    "created_at": "2026-10-24T04:20:24+00:00",
}


def _invoke(*arguments: str) -> Result:
    return CliRunner().invoke(cli, ["dataset", "task", *arguments])


@pytest.mark.description("A task ID that does not start with a letter or digit fails before any request.")
@pytest.mark.parametrize("task_id", [".hidden", "a/b", "has space"])
def test_download_rejects_invalid_task_ids_before_any_request(tracker: MockTracker, task_id: str) -> None:
    result = _invoke("download", "swe-bench/verified", task_id)

    assert result.exit_code == 2, result.output
    assert "must start with a letter or digit" in result.output
    assert tracker.requests == []


@pytest.mark.description("Commands reject a reference that names no dataset version.")
@pytest.mark.parametrize(
    ("reference", "complaint"),
    [("swe-bench", "must be BENCHMARK/DATASET"), ("swe-bench/verified@", "write a version after @")],
)
def test_commands_reject_a_reference_that_names_no_dataset_version(
    tracker: MockTracker, reference: str, complaint: str
) -> None:
    result = _invoke("list", reference)

    assert result.exit_code == 2, result.output
    assert complaint in result.output
    assert tracker.requests == []


@pytest.mark.description("List reports the tasks of the latest version or the one asked for.")
def test_list_reports_the_tasks_of_the_latest_version_or_the_one_asked_for(tracker: MockTracker) -> None:
    tracker.reply("GET", f"{_URL}/versions/latest/tasks", httpx.Response(200, json={"tasks": []}))

    assert "No tasks found." in _invoke("list", "swe-bench/verified").output

    tracker.reply("GET", f"{_URL}/versions/stable/tasks", httpx.Response(200, json={"tasks": [_OTHER, _TASK]}))
    text = _invoke("list", "swe-bench/verified@stable")
    as_json = _invoke("list", "swe-bench/verified@stable", "--format", "json")

    assert text.exit_code == 0, text.output
    assert "astropy-12907" in text.output and "django-11099" in text.output
    assert "cdcdcdcdcdcd" in text.output
    assert "Total: 2 task(s)" in text.output
    assert [task["task_id"] for task in json.loads(as_json.output)] == ["astropy-12907", "django-11099"]


@pytest.mark.description("`task download` saves the archive and exits 1 rather than overwrite an existing file.")
def test_download_saves_the_archive_and_keeps_an_existing_file(
    tracker: MockTracker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    tracker.reply(
        "GET",
        f"{_URL}/versions/2/tasks/django-11099",
        httpx.Response(200, json={**_TASK, "download_url": "https://dl.test/archive", "expires_in": 300}),
    )
    tracker.reply("GET", "/archive", httpx.Response(200, content=b"archive"))

    saved = _invoke("download", "swe-bench/verified@2", "django-11099")
    refused = _invoke("download", "swe-bench/verified@2", "django-11099")
    replaced = _invoke("download", "swe-bench/verified@2", "django-11099", "-o", "copy.zip")

    assert saved.exit_code == replaced.exit_code == 0, saved.output + replaced.output
    assert (tmp_path / "django-11099.zip").read_bytes() == b"archive"
    assert (tmp_path / "copy.zip").read_bytes() == b"archive"
    assert refused.exit_code == 1
    assert "already exists" in refused.output
