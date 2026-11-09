import gzip
import io
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from fastapi.testclient import TestClient

from main import app
from tests.utils import TEST_ORG_ID
from tracker import config
from tracker.local import config as local_config
from tracker.local.resources import LocalResources

_client = TestClient(app, base_url="http://localhost")
_DATASETS = "/v1/benchmarks/swe-bench/datasets"
_BASE = f"{_DATASETS}/verified/tasks"
_ZIP = {"content-type": "application/zip"}


def _zip(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def _archive(text: str = "task") -> bytes:
    return _zip({"instruction.md": text.encode()})


@pytest.fixture(autouse=True)
def datasets_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(local_config, "resources", LocalResources(data_root=tmp_path))
    _client.post("/v1/benchmarks", json={"name": "swe-bench"})
    created = _client.post(_DATASETS, json={"name": "verified"})
    assert created.status_code == 201
    return tmp_path / "orgs" / str(TEST_ORG_ID) / "objects" / "datasets"


@pytest.mark.description("The routes that store a dataset's tasks.")
class TestDatasetTasksApi:
    @pytest.mark.description(
        "A stored task reports its upload size, and its `download_url` returns the uploaded bytes."
    )
    def test_a_task_is_stored_under_its_dataset_and_can_be_downloaded(self, datasets_root: Path) -> None:
        body = _archive("django__django-11099")

        created = _client.put(f"{_BASE}/django__django-11099", content=body, headers=_ZIP)

        assert created.status_code == 201
        assert created.json()["task_id"] == "django__django-11099"
        assert created.json()["size"] == len(body)
        assert [path.name for path in datasets_root.glob("*/tasks/*")] == ["django__django-11099.zip"]

        fetched = _client.get(f"{_BASE}/django__django-11099")

        assert fetched.status_code == 200
        assert fetched.json()["size"] == len(body)
        assert _client.get(fetched.json()["download_url"]).content == body

    @pytest.mark.description("A second PUT of a task returns 200 and leaves one task with the new size.")
    def test_putting_a_task_again_replaces_it(self) -> None:
        _client.put(f"{_BASE}/task-1", content=_archive("first"), headers=_ZIP)
        replacement = _archive("a longer replacement")

        replaced = _client.put(f"{_BASE}/task-1", content=replacement, headers=_ZIP)

        assert replaced.status_code == 200
        assert replaced.json()["size"] == len(replacement)
        assert [task["size"] for task in _client.get(_BASE).json()["tasks"]] == [len(replacement)]

    @pytest.mark.description("Listing tasks returns them a page at a time.")
    def test_list_returns_tasks_a_page_at_a_time(self) -> None:
        for task_id in ("task-c", "task-a", "task-b"):
            _client.put(f"{_BASE}/{task_id}", content=_archive(task_id), headers=_ZIP)

        first = _client.get(_BASE, params={"limit": 2}).json()
        second = _client.get(_BASE, params={"limit": 2, "cursor": first["next_cursor"]}).json()

        assert [task["task_id"] for task in first["tasks"]] == ["task-a", "task-b"]
        assert [task["task_id"] for task in second["tasks"]] == ["task-c"]
        assert second["next_cursor"] is None

    @pytest.mark.description("Listing tasks skips objects that are not valid task archives.")
    def test_list_skips_objects_that_are_not_valid_task_archives(self, datasets_root: Path) -> None:
        _client.put(f"{_BASE}/real", content=_archive(), headers=_ZIP)
        (tasks_dir,) = datasets_root.glob("*/tasks")
        (tasks_dir / "sub").mkdir()
        for stray in ("sub/deep.zip", "x;touch.zip", "notes.txt"):
            (tasks_dir / stray).write_bytes(b"stray")

        assert [task["task_id"] for task in _client.get(_BASE).json()["tasks"]] == ["real"]

    @pytest.mark.description("Deleting a task returns it and removes it.")
    def test_delete_returns_the_task_and_removes_it(self) -> None:
        body = _archive()
        _client.put(f"{_BASE}/gone", content=body, headers=_ZIP)

        deleted = _client.delete(f"{_BASE}/gone")

        assert deleted.status_code == 200
        assert deleted.json()["size"] == len(body)
        assert _client.get(f"{_BASE}/gone").status_code == 404
        assert _client.delete(f"{_BASE}/gone").status_code == 404
        assert _client.get(_BASE).json() == {"tasks": [], "next_cursor": None}

    @pytest.mark.description("A task upload over `DATASET_TASK_UPLOAD_MAX_BYTES` returns 413 and stores nothing.")
    def test_uploads_must_be_zip_archives_under_the_size_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        assert (
            _client.put(f"{_BASE}/task-1", content=_archive(), headers={"content-type": "text/plain"}).status_code
            == 415
        )

        monkeypatch.setattr(config, "DATASET_TASK_UPLOAD_MAX_BYTES", 10)

        assert _client.put(f"{_BASE}/task-1", content=_archive("x" * 100), headers=_ZIP).status_code == 413
        assert _client.get(_BASE).json() == {"tasks": [], "next_cursor": None}

    @pytest.mark.description("A body that is not a zip archive is rejected and never stored.")
    @pytest.mark.parametrize(
        "body",
        [
            pytest.param(b"plain text", id="not-a-zip"),
            pytest.param(gzip.compress(b"task"), id="gzip"),
            pytest.param(_zip({}), id="zip-without-files"),
            pytest.param(b"PK", id="truncated-signature"),
            pytest.param(b"", id="empty"),
        ],
    )
    def test_bodies_that_are_not_zip_archives_are_rejected_and_never_stored(
        self, body: bytes, datasets_root: Path
    ) -> None:
        rejected = _client.put(f"{_BASE}/task-1", content=body, headers=_ZIP)

        assert rejected.status_code == 400
        assert list(datasets_root.glob("*/tasks/*")) == []

    @pytest.mark.description("A replacement that is not a zip returns 400 and keeps the original task size.")
    def test_a_rejected_replacement_leaves_the_stored_task_alone(self) -> None:
        original = _archive("original")
        _client.put(f"{_BASE}/task-1", content=original, headers=_ZIP)

        rejected = _client.put(f"{_BASE}/task-1", content=b"not a zip", headers=_ZIP)

        assert rejected.status_code == 400
        assert _client.get(_BASE).json()["tasks"][0]["size"] == len(original)

    @pytest.mark.description("A task PUT with a malformed task ID returns 404 or 422.")
    @pytest.mark.parametrize("task_id", ["-lead", "has space", "a/b", "x" * 129])
    def test_invalid_task_ids_are_rejected(self, task_id: str) -> None:
        assert _client.put(f"{_BASE}/{task_id}", content=_archive(), headers=_ZIP).status_code in {404, 422}

    @pytest.mark.description("Task PUT, GET, and DELETE under an unknown benchmark or dataset return 404.")
    def test_unknown_benchmarks_and_datasets_have_no_tasks(self) -> None:
        for base in (
            "/v1/benchmarks/never-existed/datasets/verified/tasks",
            f"{_DATASETS}/never-existed/tasks",
        ):
            assert _client.get(base).status_code == 404
            assert _client.put(f"{base}/task-1", content=_archive(), headers=_ZIP).status_code == 404
            assert _client.get(f"{base}/task-1").status_code == 404
            assert _client.delete(f"{base}/task-1").status_code == 404

    @pytest.mark.description(
        "Tasks of an archived dataset can be read but not written, and writes work again after a restore."
    )
    def test_an_archived_datasets_tasks_stay_readable_but_cannot_change_until_it_is_restored(self) -> None:
        body = _archive("kept")
        _client.put(f"{_BASE}/task-1", content=body, headers=_ZIP)
        _client.delete(f"{_DATASETS}/verified")

        assert [task["task_id"] for task in _client.get(_BASE).json()["tasks"]] == ["task-1"]
        assert _client.get(_client.get(f"{_BASE}/task-1").json()["download_url"]).content == body

        replaced = _client.put(f"{_BASE}/task-1", content=_archive("new"), headers=_ZIP)

        assert replaced.status_code == 409
        assert "restore it first" in replaced.json()["detail"]
        assert _client.put(f"{_BASE}/task-2", content=body, headers=_ZIP).status_code == 409
        assert _client.delete(f"{_BASE}/task-1").status_code == 409
        assert [task["size"] for task in _client.get(_BASE).json()["tasks"]] == [len(body)]

        _client.post(f"{_DATASETS}/verified/restore")

        assert _client.put(f"{_BASE}/task-2", content=body, headers=_ZIP).status_code == 201
        assert _client.delete(f"{_BASE}/task-1").status_code == 200
