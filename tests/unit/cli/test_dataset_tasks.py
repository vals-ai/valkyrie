import json
from pathlib import Path

import httpx
import pytest
from click.testing import CliRunner, Result

from valkyrie.cli.main import cli

from tests.unit.cli.conftest import MockTracker

_TASKS = "/v1/benchmarks/swe-bench/datasets/verified/tasks"
_TASK = {"task_id": "django-11099", "size": 1234, "last_modified": "2026-10-24T04:20:24+00:00"}
_OTHER = {**_TASK, "task_id": "astropy-12907", "size": 56}


def _invoke(*arguments: str, input: str | None = None) -> Result:
    return CliRunner().invoke(cli, ["dataset", "task", *arguments], input=input)


@pytest.mark.description("`task put` uploads the zip bytes and prints the stored task with its size.")
def test_put_streams_the_zip_archive_to_the_task(tracker: MockTracker, tmp_path: Path) -> None:
    archive = tmp_path / "task.zip"
    archive.write_bytes(b"PKarchive")
    tracker.reply("PUT", f"{_TASKS}/django-11099", httpx.Response(200, json=_TASK))

    result = _invoke("put", "swe-bench", "verified", "django-11099", str(archive))

    assert result.exit_code == 0, result.output
    (request,) = tracker.requests
    assert request.headers["content-type"] == "application/zip"
    assert request.content == b"PKarchive"
    assert "Task stored: swe-bench/verified/django-11099 (1,234 bytes)" in result.output


@pytest.mark.description("A task ID that does not start with a letter or digit fails before any request.")
@pytest.mark.parametrize("task_id", [".hidden", "a/b", "has space"])
def test_commands_reject_invalid_task_ids_before_any_request(
    tracker: MockTracker, tmp_path: Path, task_id: str
) -> None:
    archive = tmp_path / "task.zip"
    archive.write_bytes(b"PK")

    for arguments in (
        ("put", "swe-bench", "verified", task_id, str(archive)),
        ("download", "swe-bench", "verified", task_id),
        ("remove", "swe-bench", "verified", task_id, "-y"),
    ):
        result = _invoke(*arguments)

        assert result.exit_code == 2, result.output
        assert "must start with a letter or digit" in result.output

    assert tracker.requests == []


@pytest.mark.description("List shows the tasks or says there are none.")
def test_list_reports_the_tasks_or_that_there_are_none(tracker: MockTracker) -> None:
    tracker.reply("GET", _TASKS, httpx.Response(200, json={"tasks": [], "next_cursor": None}))

    assert "No tasks found." in _invoke("list", "swe-bench", "verified").output

    tracker.reply("GET", _TASKS, httpx.Response(200, json={"tasks": [_OTHER, _TASK], "next_cursor": None}))
    text = _invoke("list", "swe-bench", "verified")
    as_json = _invoke("list", "swe-bench", "verified", "--format", "json")

    assert text.exit_code == 0, text.output
    assert "astropy-12907" in text.output and "django-11099" in text.output
    assert "Total: 2 task(s)" in text.output
    assert [task["task_id"] for task in json.loads(as_json.output)["tasks"]] == ["astropy-12907", "django-11099"]


@pytest.mark.description("`task download` saves the archive and exits 1 rather than overwrite an existing file.")
def test_download_saves_the_archive_and_keeps_an_existing_file(
    tracker: MockTracker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    tracker.reply(
        "GET",
        f"{_TASKS}/django-11099",
        httpx.Response(200, json={**_TASK, "download_url": "https://dl.test/archive", "expires_in": 300}),
    )
    tracker.reply("GET", "/archive", httpx.Response(200, content=b"archive"))

    saved = _invoke("download", "swe-bench", "verified", "django-11099")
    refused = _invoke("download", "swe-bench", "verified", "django-11099")
    replaced = _invoke("download", "swe-bench", "verified", "django-11099", "-o", "copy.zip")

    assert saved.exit_code == replaced.exit_code == 0, saved.output + replaced.output
    assert (tmp_path / "django-11099.zip").read_bytes() == b"archive"
    assert (tmp_path / "copy.zip").read_bytes() == b"archive"
    assert refused.exit_code == 1
    assert "already exists" in refused.output


@pytest.mark.description("`task remove` asks for confirmation and deletes the task only after a yes.")
def test_remove_asks_first_and_deletes_on_yes(tracker: MockTracker) -> None:
    tracker.reply("DELETE", f"{_TASKS}/django-11099", httpx.Response(200, json=_TASK))

    declined = _invoke("remove", "swe-bench", "verified", "django-11099", input="n\n")

    assert "Cancelled." in declined.output
    assert tracker.requests == []

    confirmed = _invoke("remove", "swe-bench", "verified", "django-11099", input="y\n")

    assert confirmed.exit_code == 0, confirmed.output
    assert "Task 'swe-bench/verified/django-11099' removed." in confirmed.output
