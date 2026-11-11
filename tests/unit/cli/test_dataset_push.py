import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
from click.testing import CliRunner, Result

from valkyrie.cli.main import cli
from valkyrie.sdk.task_content import task_digest

from tests.unit.cli.conftest import MockTracker

_URL = "/v1/benchmarks/swe-bench/datasets/verified"
_RESOURCE = {
    "name": "swe-bench",
    "description": "",
    "created_at": "2026-10-24T04:20:24+00:00",
    "updated_at": "2026-10-24T04:20:24+00:00",
}
_DATASET = {**_RESOURCE, "name": "verified", "benchmark": "swe-bench"}


def _write_tasks(root: Path, *names: str) -> Path:
    for name in names:
        files = {"task.toml": "", "instruction.md": f"Do {name}.", "tests/test.sh": "#!/bin/bash\n"}
        for relative, text in files.items():
            (root / name / relative).parent.mkdir(parents=True, exist_ok=True)
            (root / name / relative).write_text(text)
    return root


def _invoke(*arguments: str, reference: str = "swe-bench/verified") -> Result:
    return CliRunner().invoke(cli, ["dataset", "push", reference, *arguments])


def _published(request: httpx.Request) -> httpx.Response:
    tasks = json.loads(request.content)["tasks"]
    version = {
        "benchmark": "swe-bench",
        "dataset": "verified",
        "revision": 3,
        "digest": "sha256:" + "cd" * 32,
        "task_count": len(tasks),
        "tags": ["latest", *json.loads(request.content)["tags"]],
        "created_at": "2026-10-24T04:20:24+00:00",
        "tasks": [{"task_id": task_id, "digest": digest, "size": 1} for task_id, digest in tasks.items()],
    }
    return httpx.Response(201, json=version)


def _reply_to_a_push(tracker: MockTracker, folder: Path, *, stored: set[str] = frozenset()) -> None:
    digests = {path.name: task_digest(path) for path in folder.iterdir()}
    tracker.reply("GET", _URL, httpx.Response(200, json=_DATASET))
    missing = [digest for task_id, digest in digests.items() if task_id not in stored]
    tracker.reply("POST", f"{_URL}/blobs/missing", httpx.Response(200, json={"missing": missing}))
    for digest in digests.values():
        tracker.reply(
            "PUT",
            f"{_URL}/blobs/{digest}",
            lambda request: httpx.Response(201, json={"digest": request.url.path.rsplit("/", 1)[1], "size": 99}),
        )
    tracker.reply("POST", f"{_URL}/versions", _published)


@pytest.mark.description("Push uploads every new task and publishes the version.")
def test_push_uploads_every_new_task_and_publishes_the_version(tracker: MockTracker, tmp_path: Path) -> None:
    folder = _write_tasks(tmp_path, "task-a", "task-b")
    _reply_to_a_push(tracker, folder)

    result = _invoke(str(folder), "--tag", "stable")

    assert result.exit_code == 0, result.output
    assert "✓ Uploaded task-a (99 bytes)" in result.output and "✓ Uploaded task-b (99 bytes)" in result.output
    assert "Published swe-bench/verified@3: 2 task(s), 2 uploaded. Tags: latest, stable." in result.output
    (published,) = tracker.requested("POST", f"{_URL}/versions")
    assert json.loads(published.content)["tags"] == ["stable"]


@pytest.mark.description("Push warns about entries left out and what the tracker warns about.")
def test_push_warns_about_entries_left_out_and_what_the_tracker_warns_about(
    tracker: MockTracker, tmp_path: Path
) -> None:
    folder = _write_tasks(tmp_path, "task-a")
    (folder / "task-a" / "data").mkdir()
    (folder / "task-a" / "notes.txt").write_text("n")
    digest = task_digest(folder / "task-a")
    _reply_to_a_push(tracker, folder)
    tracker.reply(
        "PUT",
        f"{_URL}/blobs/{digest}",
        httpx.Response(201, json={"digest": digest, "size": 99, "warnings": ["[agent].mystery is ignored"]}),
    )

    result = _invoke(str(folder))

    assert result.exit_code == 0, result.output
    assert "! task-a: not stored because they are not part of a task: data, notes.txt" in result.output
    assert "! task-a: [agent].mystery is ignored" in result.output


@pytest.mark.description("Push uploads only what the dataset does not store.")
def test_push_uploads_only_what_the_dataset_does_not_store(tracker: MockTracker, tmp_path: Path) -> None:
    folder = _write_tasks(tmp_path, "task-a", "task-b")
    _reply_to_a_push(tracker, folder, stored={"task-a"})

    result = _invoke(str(folder))

    assert result.exit_code == 0, result.output
    assert "Uploaded task-b" in result.output and "Uploaded task-a" not in result.output
    assert "2 task(s), 1 uploaded" in result.output
    assert len(tracker.requested("PUT", f"{_URL}/blobs/{task_digest(folder / 'task-a')}")) == 0


@pytest.mark.description("Push creates a missing benchmark and dataset.")
def test_push_creates_a_missing_benchmark_and_dataset(tracker: MockTracker, tmp_path: Path) -> None:
    folder = _write_tasks(tmp_path, "task-a")
    _reply_to_a_push(tracker, folder)
    tracker.reply("GET", _URL, httpx.Response(404, json={"detail": "gone"}))
    tracker.reply("GET", "/v1/benchmarks/swe-bench", httpx.Response(404, json={"detail": "gone"}))
    tracker.reply("POST", "/v1/benchmarks", httpx.Response(201, json=_RESOURCE))
    tracker.reply("POST", "/v1/benchmarks/swe-bench/datasets", httpx.Response(201, json=_DATASET))

    result = _invoke(str(folder))

    assert result.exit_code == 0, result.output
    assert "✓ Created benchmark swe-bench" in result.output and "✓ Created dataset swe-bench/verified" in result.output
    assert len(tracker.requested("POST", "/v1/benchmarks")) == 1
    assert len(tracker.requested("POST", "/v1/benchmarks/swe-bench/datasets")) == 1
    assert len(tracker.requested("POST", f"{_URL}/versions")) == 1


@pytest.mark.description("Push refuses a reference that does not name a benchmark and a dataset.")
@pytest.mark.parametrize(
    ("reference", "complaint"),
    [
        ("swe-bench", "must be BENCHMARK/DATASET"),
        ("Swe-Bench/verified", "the benchmark must be lowercase letters and digits"),
        ("swe-bench/Verified", "the dataset must be lowercase letters and digits"),
        ("swe-bench/verified/extra", "the dataset must be lowercase letters and digits"),
    ],
)
def test_push_refuses_a_reference_that_does_not_name_a_benchmark_and_a_dataset(
    tracker: MockTracker, tmp_path: Path, reference: str, complaint: str
) -> None:
    result = _invoke(str(_write_tasks(tmp_path, "task-a")), reference=reference)

    assert result.exit_code == 2
    assert complaint in result.output
    assert tracker.requests == []


@pytest.mark.description("Push reports failed uploads and publishes nothing.")
def test_push_reports_failed_uploads_and_publishes_nothing(tracker: MockTracker, tmp_path: Path) -> None:
    folder = _write_tasks(tmp_path, "task-a", "task-b")
    _reply_to_a_push(tracker, folder)
    tracker.reply(
        "PUT", f"{_URL}/blobs/{task_digest(folder / 'task-b')}", httpx.Response(403, json={"detail": "Not allowed"})
    )

    result = _invoke(str(folder))

    assert result.exit_code == 1
    assert "✓ Uploaded task-a" in result.output
    assert "✗ task-b" in result.output and "Not allowed" in result.output
    assert "1 of 2 uploads failed and nothing was published" in result.output
    assert tracker.requested("POST", f"{_URL}/versions") == []


@pytest.mark.description("Push refuses a task that changed since dataset toml was written and sends nothing.")
def test_push_refuses_a_task_that_changed_since_dataset_toml_was_written_and_sends_nothing(
    tracker: MockTracker, tmp_path: Path
) -> None:
    folder = tmp_path / "tasks"
    task = folder / "task-a"
    (task / "tests").mkdir(parents=True)
    (task / "task.toml").write_text('[task]\nname = "acme/task-a"\n')
    (task / "instruction.md").write_text("Do it.")
    (task / "tests" / "test.sh").write_text("#!/bin/bash\n")
    (folder / "dataset.toml").write_text(
        f'[dataset]\nname = "acme/verified"\n[[tasks]]\nname = "acme/task-a"\ndigest = "{task_digest(task)}"\n'
    )
    (task / "instruction.md").write_text("Changed.")

    result = _invoke(str(folder))

    assert result.exit_code == 1
    assert "Task task-a (acme/task-a) changed since dataset.toml was written" in result.output
    assert tracker.requests == []


@pytest.mark.description("Install downloads the source from its location and publishes it the way push does.")
def test_install_downloads_the_source_and_publishes_it(
    tracker: MockTracker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = _write_tasks(tmp_path, "task-a")
    _reply_to_a_push(tracker, folder)
    fetched: list[tuple[str, str]] = []

    @asynccontextmanager
    async def fetch(location: str, source: str) -> AsyncIterator[Path]:
        fetched.append((location, source))
        yield folder

    monkeypatch.setattr("valkyrie.sdk.resources.datasets.download_dataset", fetch)

    result = CliRunner().invoke(
        cli, ["dataset", "install", "swe-bench/verified", "acme/tasks", "--location", "hugging_face", "--tag", "stable"]
    )

    assert result.exit_code == 0, result.output
    assert fetched == [("hugging_face", "acme/tasks")]
    assert "Downloading acme/tasks from hugging face..." in result.output
    assert "Published swe-bench/verified@3: 1 task(s), 1 uploaded. Tags: latest, stable." in result.output


@pytest.mark.description("Install refuses a location it does not know before downloading anything.")
def test_install_refuses_an_unknown_location() -> None:
    result = CliRunner().invoke(cli, ["dataset", "install", "swe-bench/verified", "acme/tasks", "--location", "gitlab"])

    assert result.exit_code == 2
    assert "'gitlab' is not one of 'harbor', 'github', 'hugging_face'" in result.output
