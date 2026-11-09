import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from main import app
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity, get_current_starter
from tracker.database.models import BenchmarkDefinition, Org

_client = TestClient(app)
_BENCHMARKS = "/v1/benchmarks"
_DATASETS = "/v1/benchmarks/swe-bench/datasets"


@pytest.fixture(params=["benchmarks", "datasets"])
def collection(request: pytest.FixtureRequest) -> tuple[str, str]:
    if request.param == "benchmarks":
        return _BENCHMARKS, "benchmarks"
    _client.post(_BENCHMARKS, json={"name": "swe-bench"})
    return _DATASETS, "datasets"


def _rows(base: str, key: str, **params: bool) -> list[dict[str, str]]:
    return _client.get(base, params=params).json()[key]


@pytest.mark.description("The routes benchmarks and datasets share.")
class TestResourcesApi:
    @pytest.mark.description("Creating a resource returns it, and it then shows in the list.")
    def test_create_returns_the_resource_and_it_lists(self, collection: tuple[str, str]) -> None:
        base, key = collection

        created = _client.post(base, json={"name": "verified", "description": "  Issues  "})

        assert created.status_code == 201
        assert created.json()["name"] == "verified"
        assert created.json()["description"] == "Issues"
        assert _rows(base, key) == [created.json()]

    @pytest.mark.description("Creating a resource records who created it.")
    def test_create_records_who_started_it(self, collection: tuple[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
        identity = RequestIdentity(org=Org(id=TEST_ORG_ID, name="default"), email="ada@example.com")
        monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: identity)

        created = _client.post(collection[0], json={"name": "owned"})

        assert created.json()["created_by_email"] == "ada@example.com"

    @pytest.mark.description("Creating a resource with a used name returns 409, even after that resource is archived.")
    def test_a_taken_name_stays_taken_after_the_resource_is_archived(self, collection: tuple[str, str]) -> None:
        base, _ = collection
        _client.post(base, json={"name": "taken"})

        conflict = _client.post(base, json={"name": "taken"})

        assert conflict.status_code == 409
        assert "'taken'" in conflict.json()["detail"]

        assert _client.delete(f"{base}/taken").status_code == 200
        assert _client.post(base, json={"name": "taken"}).status_code == 409

    @pytest.mark.description("Creating a resource with an invalid name is rejected.")
    @pytest.mark.parametrize("name", ["Upper", "has space", "-lead", "trail-", "dou..ble", "x" * 65, ""])
    def test_create_rejects_invalid_names(self, collection: tuple[str, str], name: str) -> None:
        base, key = collection

        assert _client.post(base, json={"name": name}).status_code == 422
        assert _rows(base, key) == []

    @pytest.mark.description("A PATCH changes only the description, and an empty PATCH returns 422.")
    def test_update_changes_the_description_and_keeps_the_name(self, collection: tuple[str, str]) -> None:
        base, key = collection
        _client.post(base, json={"name": "docs", "description": "before"})

        updated = _client.patch(f"{base}/docs", json={"description": "after"})

        assert updated.status_code == 200
        assert updated.json()["name"] == "docs"
        assert [row["description"] for row in _rows(base, key)] == ["after"]
        assert _client.patch(f"{base}/docs", json={}).status_code == 422

    @pytest.mark.description("Unknown resources return 404.")
    def test_unknown_resources_are_not_found(self, collection: tuple[str, str]) -> None:
        base, key = collection

        assert _client.get(f"{base}/never-existed").status_code == 404
        assert _client.patch(f"{base}/never-existed", json={"description": "x"}).status_code == 404
        assert _client.delete(f"{base}/never-existed").status_code == 404
        assert _client.post(f"{base}/never-existed/restore").status_code == 404
        assert _client.patch(f"{base}/Bad Name", json={"description": "x"}).status_code == 422
        assert _rows(base, key) == []

    @pytest.mark.description("An archived resource stays readable but cannot change until it is restored.")
    def test_an_archived_resource_stays_readable_but_read_only_until_it_is_restored(
        self, collection: tuple[str, str]
    ) -> None:
        base, key = collection
        created = _client.post(base, json={"name": "old", "description": "before"}).json()
        assert created["archived_at"] is None

        archived = _client.delete(f"{base}/old")

        assert archived.status_code == 200
        assert archived.json()["archived_at"] is not None
        assert _client.delete(f"{base}/old").json() == archived.json()
        assert _rows(base, key) == []
        assert _rows(base, key, include_archived=True) == [archived.json()]
        assert _client.get(f"{base}/old").json() == archived.json()

        edited = _client.patch(f"{base}/old", json={"description": "after"})

        assert edited.status_code == 409
        assert "restore it first" in edited.json()["detail"]

        restored = _client.post(f"{base}/old/restore")

        assert restored.status_code == 200
        assert restored.json()["archived_at"] is None
        assert _client.post(f"{base}/old/restore").json() == restored.json()
        assert _rows(base, key) == [restored.json()]
        assert _client.patch(f"{base}/old", json={"description": "after"}).status_code == 200


@pytest.mark.description("What only benchmarks do.")
class TestBenchmarksApi:
    @pytest.mark.description("A benchmark can be archived only after its datasets are.")
    def test_archiving_waits_until_the_benchmarks_datasets_are_archived(self) -> None:
        _client.post(_BENCHMARKS, json={"name": "parent"})
        _client.post(f"{_BENCHMARKS}/parent/datasets", json={"name": "child"})

        blocked = _client.delete(f"{_BENCHMARKS}/parent")

        assert blocked.status_code == 409
        assert "still has active datasets" in blocked.json()["detail"]
        assert [row["name"] for row in _rows(_BENCHMARKS, "benchmarks")] == ["parent"]

        _client.delete(f"{_BENCHMARKS}/parent/datasets/child")

        assert _client.delete(f"{_BENCHMARKS}/parent").status_code == 200

    @pytest.mark.description(
        "An archived benchmark keeps its datasets readable and takes no new ones until it is restored."
    )
    def test_an_archived_benchmark_keeps_its_datasets_readable_and_takes_no_new_ones_until_restored(self) -> None:
        base = f"{_BENCHMARKS}/parent/datasets"
        _client.post(_BENCHMARKS, json={"name": "parent"})
        _client.post(base, json={"name": "child"})
        _client.delete(f"{base}/child")
        _client.delete(f"{_BENCHMARKS}/parent")

        assert _client.get(f"{base}/child").json()["archived_at"] is not None
        assert [row["name"] for row in _rows(base, "datasets", include_archived=True)] == ["child"]
        assert _client.post(base, json={"name": "other"}).status_code == 409
        assert _client.post(f"{base}/child/restore").status_code == 409

        _client.post(f"{_BENCHMARKS}/parent/restore")

        assert _client.post(f"{base}/child/restore").status_code == 200
        assert _client.post(base, json={"name": "other"}).status_code == 201

    @pytest.mark.description("Another organization's benchmarks are invisible.")
    def test_another_organizations_benchmarks_are_invisible(self, database_session: Session) -> None:
        other_org = Org(name="other-org")
        database_session.add(other_org)
        database_session.commit()
        database_session.add(BenchmarkDefinition(org_id=other_org.id, name="private"))
        database_session.commit()

        assert _client.get(_BENCHMARKS, params={"include_archived": True}).json() == {"benchmarks": []}
        assert _client.get(f"{_BENCHMARKS}/private").status_code == 404
        assert _client.patch(f"{_BENCHMARKS}/private", json={"description": "x"}).status_code == 404
        assert _client.delete(f"{_BENCHMARKS}/private").status_code == 404
        assert _client.post(f"{_BENCHMARKS}/private/restore").status_code == 404
        assert _client.post(_BENCHMARKS, json={"name": "private"}).status_code == 201


@pytest.mark.description(
    "What only datasets do: be read one at a time, and belong to one benchmark of the organization."
)
class TestDatasetsApi:
    @pytest.mark.description(
        "A dataset GET returns archived datasets with `archived_at` set, 404 for unknown names, and 422 for invalid names."
    )
    def test_one_dataset_reads_back_whether_or_not_it_is_archived(self) -> None:
        _client.post(_BENCHMARKS, json={"name": "swe-bench"})
        created = _client.post(_DATASETS, json={"name": "verified"})

        assert _client.get(f"{_DATASETS}/verified").json() == created.json()

        _client.delete(f"{_DATASETS}/verified")

        assert _client.get(f"{_DATASETS}/verified").json()["archived_at"] is not None
        assert _client.get(f"{_DATASETS}/never-existed").status_code == 404
        assert _client.get(f"{_DATASETS}/Bad Name").status_code == 422

    @pytest.mark.description(
        "A dataset names its benchmark, and two benchmarks can each have a dataset with the same name."
    )
    def test_a_dataset_names_its_benchmark_and_the_same_name_can_belong_to_two(self) -> None:
        _client.post(_BENCHMARKS, json={"name": "swe-bench"})
        _client.post(_BENCHMARKS, json={"name": "terminal-bench"})
        _client.post(_DATASETS, json={"name": "default"})

        other = _client.post(f"{_BENCHMARKS}/terminal-bench/datasets", json={"name": "default"})

        assert other.status_code == 201
        assert other.json()["benchmark"] == "terminal-bench"
        assert [row["benchmark"] for row in _client.get(_DATASETS).json()["datasets"]] == ["swe-bench"]

    @pytest.mark.description("Dataset update, delete, and restore under an unknown benchmark return 404.")
    def test_an_unknown_benchmark_has_no_datasets(self) -> None:
        base = f"{_BENCHMARKS}/never-existed/datasets"

        assert _client.post(base, json={"name": "x"}).status_code == 404
        assert _client.get(base).status_code == 404
        assert _client.get(f"{base}/x").status_code == 404
        assert _client.patch(f"{base}/x", json={"description": "x"}).status_code == 404
        assert _client.delete(f"{base}/x").status_code == 404
        assert _client.post(f"{base}/x/restore").status_code == 404

    @pytest.mark.description("Another organization's benchmark and its datasets are invisible.")
    def test_another_organizations_benchmark_is_invisible(self, database_session: Session) -> None:
        other_org = Org(name="other-org")
        database_session.add(other_org)
        database_session.commit()
        database_session.add(BenchmarkDefinition(org_id=other_org.id, name="private"))
        database_session.commit()
        base = f"{_BENCHMARKS}/private/datasets"

        assert _client.post(base, json={"name": "x"}).status_code == 404
        assert _client.get(base).status_code == 404
        assert _client.get(f"{base}/x").status_code == 404
