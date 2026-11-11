from typing import Any
import gzip
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from main import app
from tests.unit.datasets.support import archive_digest_of, publish_version, zip_bytes
from tests.utils import TEST_ORG_ID
from tracker import config
from tracker.local import config as local_config
from tracker.local.resources import LocalResources

_client = TestClient(app, base_url="http://localhost")
_DATASETS = "/v1/benchmarks/swe-bench/datasets"
_BASE = f"{_DATASETS}/verified"
_ZIP = {"content-type": "application/zip"}


def _archive(text: str = "task", toml: str = "[environment]\ndocker_image = 'ubuntu:24.04'") -> bytes:
    return zip_bytes({"task.toml": toml.encode(), "instruction.md": text.encode(), "tests/test.sh": b"#!/bin/bash"})


def _blob_files(root: Path) -> list[str]:
    return sorted(path.name for path in root.glob("*/blobs/sha256/*"))


@pytest.fixture(autouse=True)
def datasets_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(local_config, "resources", LocalResources(data_root=tmp_path))
    _client.post("/v1/benchmarks", json={"name": "swe-bench"})
    created = _client.post(_DATASETS, json={"name": "verified"})
    assert created.status_code == 201
    return tmp_path / "orgs" / str(TEST_ORG_ID) / "objects" / "datasets"


def _tasks(ref: str = "1") -> list[dict[str, Any]]:
    return _client.get(f"{_BASE}/versions/{ref}/tasks").json()["tasks"]


@pytest.mark.description("Storing a task's archive under the digest of its files.")
class TestBlobs:
    @pytest.mark.description("An archive is stored once under its dataset and digest.")
    def test_an_archive_is_stored_once_under_its_dataset_and_digest(self, datasets_root: Path) -> None:
        body = _archive("django__django-11099")
        digest = archive_digest_of(body)

        created = _client.put(f"{_BASE}/blobs/{digest}", content=body, headers=_ZIP)
        again = _client.put(f"{_BASE}/blobs/{digest}", content=body, headers=_ZIP)

        assert created.status_code == 201
        assert created.json() == {"digest": digest, "size": len(body), "warnings": []}
        assert again.status_code == 200
        assert _blob_files(datasets_root) == [f"{digest.removeprefix('sha256:')}.zip"]

    @pytest.mark.description("Missing returns only the digests the dataset does not store.")
    def test_missing_returns_only_the_digests_the_dataset_does_not_store(self) -> None:
        stored = _archive("stored")
        absent = archive_digest_of(_archive("absent"))
        _client.put(f"{_BASE}/blobs/{archive_digest_of(stored)}", content=stored, headers=_ZIP)

        response = _client.post(f"{_BASE}/blobs/missing", json={"digests": [archive_digest_of(stored), absent, absent]})

        assert response.status_code == 200
        assert response.json() == {"missing": [absent]}

    @pytest.mark.description("An archive that does not hash to its digest is refused and never stored.")
    def test_an_archive_that_does_not_hash_to_its_digest_is_refused_and_never_stored(self, datasets_root: Path) -> None:
        wrong = archive_digest_of(_archive("another"))

        refused = _client.put(f"{_BASE}/blobs/{wrong}", content=_archive("task"), headers=_ZIP)

        assert refused.status_code == 422
        assert wrong in refused.json()["detail"]
        assert _blob_files(datasets_root) == []

    @pytest.mark.description("A task that cannot run is refused and never stored.")
    def test_a_task_that_cannot_run_is_refused_and_never_stored(self, datasets_root: Path) -> None:
        body = zip_bytes({"task.toml": b"[environment]\ncpus = 0", "instruction.md": b"Do it."})

        refused = _client.put(f"{_BASE}/blobs/{archive_digest_of(body)}", content=body, headers=_ZIP)

        assert refused.status_code == 422
        assert "[environment].cpus" in refused.json()["detail"]
        assert "tests/test.sh is missing" in refused.json()["detail"]
        assert _blob_files(datasets_root) == []

    @pytest.mark.description("A stored task comes back with its warnings.")
    def test_a_stored_task_comes_back_with_its_warnings(self) -> None:
        body = _archive(toml="[environment]\ndocker_image = 'ubuntu:24.04'\nmystery = 1")

        stored = _client.put(f"{_BASE}/blobs/{archive_digest_of(body)}", content=body, headers=_ZIP)

        assert stored.status_code == 201
        assert stored.json()["warnings"] == [
            "[environment].mystery is not a field this version knows, so it is ignored"
        ]

    @pytest.mark.description("Uploads must be zip archives under the size limit.")
    def test_uploads_must_be_zip_archives_under_the_size_limit(
        self, datasets_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        body = _archive("x" * 100)
        digest = archive_digest_of(body)

        wrong_type = _client.put(f"{_BASE}/blobs/{digest}", content=body, headers={"content-type": "text/plain"})

        assert wrong_type.status_code == 415

        monkeypatch.setattr(config, "DATASET_TASK_UPLOAD_MAX_BYTES", 10)

        assert _client.put(f"{_BASE}/blobs/{digest}", content=body, headers=_ZIP).status_code == 413
        assert _blob_files(datasets_root) == []

    @pytest.mark.description("Bodies that are not zip archives with files are rejected and never stored.")
    @pytest.mark.parametrize(
        "body",
        [
            pytest.param(b"plain text", id="not-a-zip"),
            pytest.param(gzip.compress(b"task"), id="gzip"),
            pytest.param(zip_bytes({}), id="zip-without-files"),
            pytest.param(b"PK", id="truncated-signature"),
            pytest.param(b"", id="empty"),
        ],
    )
    def test_bodies_that_are_not_zip_archives_with_files_are_rejected_and_never_stored(
        self, body: bytes, datasets_root: Path
    ) -> None:
        digest = archive_digest_of(_archive())

        rejected = _client.put(f"{_BASE}/blobs/{digest}", content=body, headers=_ZIP)

        assert rejected.status_code in {400, 422}
        assert _blob_files(datasets_root) == []

    @pytest.mark.description("A path that is not a digest is rejected.")
    @pytest.mark.parametrize("digest", ["sha256:abc", "md5:" + "0" * 32, "0" * 64, "sha256:" + "G" * 64])
    def test_a_path_that_is_not_a_digest_is_rejected(self, digest: str) -> None:
        assert _client.put(f"{_BASE}/blobs/{digest}", content=_archive(), headers=_ZIP).status_code in {404, 422}


@pytest.mark.description("Publishing versions and reading them back.")
class TestVersions:
    @pytest.mark.description("Publishing records a numbered version tagged latest.")
    def test_publishing_records_a_numbered_version_tagged_latest(self) -> None:
        published = publish_version(_client, _BASE, {"task-a": _archive("a"), "task-b": _archive("b")})

        assert published["revision"] == 1
        assert published["task_count"] == 2
        assert published["tags"] == ["latest"]
        assert published["digest"].startswith("sha256:")
        assert [task["task_id"] for task in _tasks()] == ["task-a", "task-b"]
        assert _tasks()[0]["digest"] == archive_digest_of(_archive("a"))

    @pytest.mark.description("Publishing the same tasks again returns the same version.")
    def test_publishing_the_same_tasks_again_returns_the_same_version(self, datasets_root: Path) -> None:
        archives = {"task-a": _archive("a"), "task-b": _archive("b")}
        first = publish_version(_client, _BASE, archives)

        again = _client.post(
            f"{_BASE}/versions", json={"tasks": {t: archive_digest_of(a) for t, a in archives.items()}}
        )

        assert again.status_code == 200
        assert again.json()["revision"] == first["revision"] == 1
        assert again.json()["digest"] == first["digest"]
        assert len(_client.get(f"{_BASE}/versions").json()["versions"]) == 1

    @pytest.mark.description("Changing one task makes a new revision and uploads only that archive.")
    def test_changing_one_task_makes_a_new_revision_and_uploads_only_that_archive(self, datasets_root: Path) -> None:
        publish_version(_client, _BASE, {"task-a": _archive("a"), "task-b": _archive("b")})
        before = _blob_files(datasets_root)

        changed = {"task-a": _archive("a"), "task-b": _archive("b changed")}
        digests = {task: archive_digest_of(archive) for task, archive in changed.items()}
        missing = _client.post(f"{_BASE}/blobs/missing", json={"digests": list(digests.values())}).json()["missing"]

        assert missing == [digests["task-b"]]

        second = publish_version(_client, _BASE, changed)

        assert second["revision"] == 2
        assert len(_blob_files(datasets_root)) == len(before) + 1
        listed = _client.get(f"{_BASE}/versions").json()["versions"]
        assert [(version["revision"], version["tags"]) for version in listed] == [(2, ["latest"]), (1, [])]

    @pytest.mark.description("Going back to earlier tasks returns the earlier revision and moves latest.")
    def test_going_back_to_earlier_tasks_returns_the_earlier_revision_and_moves_latest(self) -> None:
        first = publish_version(_client, _BASE, {"task-a": _archive("a")})
        publish_version(_client, _BASE, {"task-a": _archive("changed")})

        back = publish_version(_client, _BASE, {"task-a": _archive("a")})

        assert back["revision"] == first["revision"] == 1
        assert back["tags"] == ["latest"]
        assert _client.get(f"{_BASE}/versions/latest").json()["revision"] == 1

    @pytest.mark.description("A version is found by revision tag and digest prefix.")
    def test_a_version_is_found_by_revision_tag_and_digest_prefix(self) -> None:
        published = publish_version(_client, _BASE, {"task-a": _archive("a")}, tags=["stable"])

        for ref in ("1", "latest", "stable", published["digest"], published["digest"][:15]):
            found = _client.get(f"{_BASE}/versions/{ref}")

            assert found.status_code == 200, ref
            assert found.json()["digest"] == published["digest"]
        assert found.json()["tags"] == ["latest", "stable"]

    @pytest.mark.description("An unknown version is not found.")
    @pytest.mark.parametrize("ref", ["2", "unknown", "sha256:" + "0" * 64])
    def test_an_unknown_version_is_not_found(self, ref: str) -> None:
        publish_version(_client, _BASE, {"task-a": _archive("a")})

        assert _client.get(f"{_BASE}/versions/{ref}").status_code == 404

    @pytest.mark.description("A reference that cannot be read is a bad request.")
    @pytest.mark.parametrize("ref", ["sha256:abc", "Not A Tag", "sha256:" + "z" * 12])
    def test_a_reference_that_cannot_be_read_is_a_bad_request(self, ref: str) -> None:
        assert _client.get(f"{_BASE}/versions/{ref}").status_code == 400

    @pytest.mark.description("Publishing needs every archive to be stored first.")
    def test_publishing_needs_every_archive_to_be_stored_first(self) -> None:
        stored = _archive("stored")
        absent = archive_digest_of(_archive("absent"))
        _client.put(f"{_BASE}/blobs/{archive_digest_of(stored)}", content=stored, headers=_ZIP)

        refused = _client.post(
            f"{_BASE}/versions", json={"tasks": {"task-a": archive_digest_of(stored), "task-b": absent}}
        )

        assert refused.status_code == 409
        assert absent in refused.json()["detail"]
        assert _client.get(f"{_BASE}/versions").json() == {"versions": []}

    @pytest.mark.description("A version needs valid task ids and digests.")
    @pytest.mark.parametrize(
        "tasks",
        [{}, {"-lead": "sha256:" + "0" * 64}, {"task-a": "sha256:short"}, {"task-a": "0" * 64}],
        ids=["no-tasks", "bad-task-id", "short-digest", "no-prefix"],
    )
    def test_a_version_needs_valid_task_ids_and_digests(self, tasks: dict[str, str]) -> None:
        assert _client.post(f"{_BASE}/versions", json={"tasks": tasks}).status_code == 422

    @pytest.mark.description("A version task can be downloaded.")
    def test_a_version_task_can_be_downloaded(self) -> None:
        body = _archive("django__django-11099")
        publish_version(_client, _BASE, {"django__django-11099": body})

        fetched = _client.get(f"{_BASE}/versions/1/tasks/django__django-11099")

        assert fetched.status_code == 200
        assert fetched.json()["digest"] == archive_digest_of(body)
        assert fetched.json()["size"] == len(body)
        assert _client.get(fetched.json()["download_url"]).content == body
        assert _client.get(f"{_BASE}/versions/1/tasks/other").status_code == 404


@pytest.mark.description("The dataset.toml a version is published with.")
class TestManifest:
    @pytest.mark.description("A manifest is kept with the version and each tasks source name.")
    def test_a_manifest_is_kept_with_the_version_and_each_tasks_source_name(self) -> None:
        manifest = {
            "name": "org/verified",
            "version": "1.0",
            "description": "Checked issues",
            "authors": [{"name": "Ada", "email": "ada@example.com"}],
            "keywords": ["code"],
            "metadata": {"source": "https://example.test"},
        }

        published = publish_version(_client, _BASE, {"task-a": _archive("a")}, manifest=manifest)
        fetched = _client.get(f"{_BASE}/versions/1").json()

        assert published["manifest"] == fetched["manifest"] == manifest
        assert [task["source_name"] for task in _tasks()] == ["org/task-a"]

    @pytest.mark.description("Tasks are listed in pages in task ID order and the last page has no cursor.")
    def test_tasks_are_listed_in_pages_in_task_id_order_and_the_last_page_has_no_cursor(self) -> None:
        publish_version(_client, _BASE, {"task-c": _archive("c"), "task-a": _archive("a"), "task-b": _archive("b")})

        first = _client.get(f"{_BASE}/versions/1/tasks", params={"limit": 2}).json()
        rest = _client.get(f"{_BASE}/versions/1/tasks", params={"limit": 2, "cursor": first["next_cursor"]}).json()

        assert [task["task_id"] for task in first["tasks"]] == ["task-a", "task-b"]
        assert first["next_cursor"] == "task-b"
        assert [task["task_id"] for task in rest["tasks"]] == ["task-c"] and rest["next_cursor"] is None

    @pytest.mark.description("A version without a manifest has none.")
    def test_a_version_without_a_manifest_has_none(self) -> None:
        published = publish_version(_client, _BASE, {"task-a": _archive("a")})

        assert published["manifest"] is None
        assert _tasks()[0]["source_name"] is None

    @pytest.mark.description("Publishing the same tasks with a manifest adds it to a version that has none.")
    def test_publishing_the_same_tasks_with_a_manifest_adds_it_to_a_version_that_has_none(self) -> None:
        publish_version(_client, _BASE, {"task-a": _archive("a")})

        again = publish_version(_client, _BASE, {"task-a": _archive("a")}, manifest={"name": "org/verified"})

        assert again["revision"] == 1
        assert again["manifest"]["name"] == "org/verified"
        assert _tasks()[0]["source_name"] == "org/task-a"

    @pytest.mark.description("Re-publishing the same tasks with an edited manifest replaces the stored details.")
    def test_an_edited_manifest_replaces_the_stored_one(self) -> None:
        publish_version(_client, _BASE, {"task-a": _archive("a")}, manifest={"name": "org/first"})

        digest = archive_digest_of(_archive("a"))
        task = {"name": "other/task-a", "digest": digest}
        manifest = {"name": "other/second", "tasks": {"task-a": task}}
        again = _client.post(f"{_BASE}/versions", json={"tasks": {"task-a": digest}, "manifest": manifest}).json()

        assert again["revision"] == 1
        assert again["manifest"]["name"] == "other/second"
        assert _tasks()[0]["source_name"] == "other/task-a"

    @pytest.mark.description("The tasks must be exactly the ones the manifest lists.")
    @pytest.mark.parametrize(
        ("tasks", "listed", "complaint"),
        [
            (["task-a", "task-b"], ["task-a"], "does not list: task-b"),
            (["task-a"], ["task-a", "task-b"], "lists tasks that are not published: task-b"),
        ],
        ids=["unlisted-task", "unpublished-task"],
    )
    def test_the_tasks_must_be_exactly_the_ones_the_manifest_lists(
        self, tasks: list[str], listed: list[str], complaint: str
    ) -> None:
        digests = {task_id: archive_digest_of(_archive(task_id)) for task_id in {*tasks, *listed}}
        for task_id in tasks:
            _client.put(f"{_BASE}/blobs/{digests[task_id]}", content=_archive(task_id), headers=_ZIP)
        manifest = {
            "name": "org/verified",
            "tasks": {task_id: {"name": f"org/{task_id}", "digest": digests[task_id]} for task_id in listed},
        }

        refused = _client.post(
            f"{_BASE}/versions", json={"tasks": {task_id: digests[task_id] for task_id in tasks}, "manifest": manifest}
        )

        assert refused.status_code == 422
        assert complaint in refused.json()["detail"]
        assert _client.get(f"{_BASE}/versions").json() == {"versions": []}

    @pytest.mark.description("A digest that differs from the one listed is refused naming both.")
    def test_a_digest_that_differs_from_the_one_listed_is_refused_naming_both(self) -> None:
        stored = archive_digest_of(_archive("a"))
        listed = archive_digest_of(_archive("changed"))
        _client.put(f"{_BASE}/blobs/{stored}", content=_archive("a"), headers=_ZIP)
        manifest = {"name": "org/verified", "tasks": {"task-a": {"name": "org/task-a", "digest": listed}}}

        refused = _client.post(f"{_BASE}/versions", json={"tasks": {"task-a": stored}, "manifest": manifest})

        assert refused.status_code == 422
        assert "Task 'task-a'" in refused.json()["detail"]
        assert stored in refused.json()["detail"] and listed in refused.json()["detail"]

    @pytest.mark.description("A manifest that pins dataset files is refused.")
    def test_a_manifest_that_pins_dataset_files_is_refused(self) -> None:
        digest = archive_digest_of(_archive("a"))
        _client.put(f"{_BASE}/blobs/{digest}", content=_archive("a"), headers=_ZIP)
        manifest = {
            "name": "org/verified",
            "tasks": {"task-a": {"name": "org/task-a", "digest": digest}},
            "files": [{"path": "metric.py", "digest": digest}],
        }

        refused = _client.post(f"{_BASE}/versions", json={"tasks": {"task-a": digest}, "manifest": manifest})

        assert refused.status_code == 422
        assert "dataset files" in refused.text


@pytest.mark.description("Naming versions.")
class TestTags:
    @pytest.mark.description("A tag is set, moved only with `force`, and removed.")
    def test_a_tag_is_set_moved_and_removed(self) -> None:
        publish_version(_client, _BASE, {"task-a": _archive("a")})
        publish_version(_client, _BASE, {"task-a": _archive("b")})

        set_tag = _client.put(f"{_BASE}/tags/stable", json={"ref": "1"})
        same = _client.put(f"{_BASE}/tags/stable", json={"ref": "1"})
        refused = _client.put(f"{_BASE}/tags/stable", json={"ref": "2"})
        moved = _client.put(f"{_BASE}/tags/stable", json={"ref": "2", "force": True})

        assert set_tag.status_code == same.status_code == 200
        assert set_tag.json()["tags"] == ["stable"]
        assert refused.status_code == 409
        assert "force" in refused.json()["detail"]
        assert moved.json()["revision"] == 2
        assert moved.json()["tags"] == ["latest", "stable"]

        removed = _client.delete(f"{_BASE}/tags/stable")

        assert removed.status_code == 200
        assert removed.json()["revision"] == 2
        assert _client.get(f"{_BASE}/versions/stable").status_code == 404
        assert _client.delete(f"{_BASE}/tags/stable").status_code == 404

    @pytest.mark.description("The latest tag cannot be removed.")
    def test_the_latest_tag_cannot_be_removed(self) -> None:
        publish_version(_client, _BASE, {"task-a": _archive("a")})

        refused = _client.delete(f"{_BASE}/tags/latest")

        assert refused.status_code == 409
        assert _client.get(f"{_BASE}/versions/latest").status_code == 200

    @pytest.mark.description("Tags that could be mistaken for other references are refused.")
    @pytest.mark.parametrize("tag", ["3", "Stable", "-x", "sha256:abcdef12", "a_b"])
    def test_tags_that_could_be_mistaken_for_other_references_are_refused(self, tag: str) -> None:
        publish_version(_client, _BASE, {"task-a": _archive("a")})

        assert _client.put(f"{_BASE}/tags/{tag}", json={"ref": "1"}).status_code in {400, 404}

    @pytest.mark.description("Tagging an unknown version is not found.")
    def test_tagging_an_unknown_version_is_not_found(self) -> None:
        publish_version(_client, _BASE, {"task-a": _archive("a")})

        assert _client.put(f"{_BASE}/tags/stable", json={"ref": "9"}).status_code == 404

    @pytest.mark.description("The `latest` tag moves without `force`.")
    def test_the_latest_tag_moves_without_force(self) -> None:
        publish_version(_client, _BASE, {"task-a": _archive("a")})
        publish_version(_client, _BASE, {"task-a": _archive("b")})

        moved = _client.put(f"{_BASE}/tags/latest", json={"ref": "1"})

        assert moved.status_code == 200
        assert moved.json()["revision"] == 1

    @pytest.mark.description("Publishing with a tag that points at another version is refused and publishes nothing.")
    def test_publishing_with_a_tag_of_another_version_is_refused(self) -> None:
        publish_version(_client, _BASE, {"task-a": _archive("a")}, tags=["stable"])
        digest = archive_digest_of(_archive("b"))
        _client.put(f"{_BASE}/blobs/{digest}", content=_archive("b"), headers=_ZIP)

        refused = _client.post(f"{_BASE}/versions", json={"tasks": {"task-a": digest}, "tags": ["stable"]})

        assert refused.status_code == 409
        assert [row["revision"] for row in _client.get(f"{_BASE}/versions").json()["versions"]] == [1]

    @pytest.mark.description("Publishing with tags points them at the version.")
    def test_publishing_with_tags_points_them_at_the_version(self) -> None:
        publish_version(_client, _BASE, {"task-a": _archive("a")}, tags=["stable"])
        second = publish_version(_client, _BASE, {"task-a": _archive("b")}, tags=["next", "next"])

        assert second["tags"] == ["latest", "next"]
        assert _client.get(f"{_BASE}/versions/stable").json()["revision"] == 1

    @pytest.mark.description("A version has at most ten tags.")
    def test_a_version_has_at_most_ten_tags(self) -> None:
        digest = archive_digest_of(_archive())
        tags = [f"tag-{number}" for number in range(11)]

        response = _client.post(f"{_BASE}/versions", json={"tasks": {"task-a": digest}, "tags": tags})

        assert response.status_code == 422


@pytest.mark.description("Datasets that are missing or archived.")
class TestUnknownAndArchived:
    @pytest.mark.description("Unknown benchmarks and datasets have nothing to read or write.")
    def test_unknown_benchmarks_and_datasets_have_nothing_to_read_or_write(self) -> None:
        digest = archive_digest_of(_archive())
        for base in ("/v1/benchmarks/never-existed/datasets/verified", f"{_DATASETS}/never-existed"):
            assert _client.get(f"{base}/versions").status_code == 404
            assert _client.get(f"{base}/versions/1").status_code == 404
            assert _client.post(f"{base}/blobs/missing", json={"digests": [digest]}).status_code == 404
            assert _client.put(f"{base}/blobs/{digest}", content=_archive(), headers=_ZIP).status_code == 404
            assert _client.post(f"{base}/versions", json={"tasks": {"task-a": digest}}).status_code == 404
            assert _client.put(f"{base}/tags/stable", json={"ref": "1"}).status_code == 404
            assert _client.delete(f"{base}/tags/stable").status_code == 404

    @pytest.mark.description("An archived datasets versions stay readable but cannot change until it is restored.")
    def test_an_archived_datasets_versions_stay_readable_but_cannot_change_until_it_is_restored(self) -> None:
        body = _archive("kept")
        publish_version(_client, _BASE, {"task-a": body})
        _client.delete(_BASE)

        assert _client.get(f"{_BASE}/versions/1").json()["task_count"] == 1
        assert _client.get(_client.get(f"{_BASE}/versions/1/tasks/task-a").json()["download_url"]).content == body

        other = _archive("new")
        digest = archive_digest_of(other)
        refusals = [
            _client.put(f"{_BASE}/blobs/{digest}", content=other, headers=_ZIP),
            _client.post(f"{_BASE}/versions", json={"tasks": {"task-a": archive_digest_of(body)}, "tags": ["x"]}),
            _client.put(f"{_BASE}/tags/stable", json={"ref": "1"}),
            _client.delete(f"{_BASE}/tags/latest"),
        ]

        assert [refusal.status_code for refusal in refusals] == [409, 409, 409, 409]
        assert "restore it first" in refusals[0].json()["detail"]

        _client.post(f"{_BASE}/restore")

        assert _client.put(f"{_BASE}/blobs/{digest}", content=other, headers=_ZIP).status_code == 201
