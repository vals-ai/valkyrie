import httpx
import pytest

from datetime import datetime, timedelta, timezone

from benchmark_service.schemas import DatasetVersion
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import link_run_project, make_benchmark, make_project
from tests.project_service import MockProjectRoutes
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import (
    Benchmark,
    Org,
)
from tracker.clients.projects import ProjectServiceClient, UnavailableAwareTransport
from tracker.domain import BenchmarkStatus
from tracker.repositories import project as project_repository
from tracker.repositories.project import ProjectRepository
from tracker.services import results as results_module
from tracker.services.datasets.ref import HostedDataset
from tracker.services.runs.regrade import RegradeTasks, regrade_run


def _seed_benchmarks(
    database_session: Session,
    count: int,
    started_by_email: str | None = None,
    start_status: BenchmarkStatus | None = None,
) -> list[Benchmark]:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    benchmarks: list[Benchmark] = []
    for benchmark_index in range(count):
        status = start_status
        if status is None:
            status = BenchmarkStatus.IN_PROGRESS if benchmark_index % 2 == 0 else BenchmarkStatus.FINISHED

        benchmark = make_benchmark(
            name=f"bench-{benchmark_index}",
            status=status,
            started_at=base + timedelta(minutes=benchmark_index),
            started_by_email=started_by_email,
        )
        database_session.add(benchmark)
        benchmarks.append(benchmark)
    database_session.commit()

    return benchmarks


@pytest.mark.description("Benchmark listing pagination, filters, and response fields.")
class TestFetchBenchmarks:
    @pytest.mark.description(
        (
            "Cursor pagination must return every benchmark once in stable order.\n"
            "\n"
            "Test cases:\n"
            "- Following the next cursor returns the remaining rows without overlap."
        )
    )
    def test_keyset_paginates_with_cursor(
        self,
        access_key_client: TestClient,
        database_session: Session,
    ) -> None:
        seeded_benchmarks = _seed_benchmarks(database_session, 5)
        first_response = access_key_client.get(
            "/fetch-benchmarks?limit=2&cursor=",
            headers={"x-api-key": "fake-key"},
        )

        assert first_response.status_code == 200, first_response.text
        first_page = first_response.json()
        assert len(first_page["benchmarks"]) == 2
        assert first_page["next_cursor"] is not None

        second_response = access_key_client.get(
            f"/fetch-benchmarks?limit=2&cursor={first_page['next_cursor']}",
            headers={"x-api-key": "fake-key"},
        )

        assert second_response.status_code == 200, second_response.text
        second_page = second_response.json()
        assert len(second_page["benchmarks"]) == 2
        assert second_page["next_cursor"] is not None

        third_response = access_key_client.get(
            f"/fetch-benchmarks?limit=2&cursor={second_page['next_cursor']}",
            headers={"x-api-key": "fake-key"},
        )

        assert third_response.status_code == 200, third_response.text
        third_page = third_response.json()
        assert len(third_page["benchmarks"]) == 1
        assert third_page["next_cursor"] is None

        returned_ids = {
            benchmark["id"] for page in (first_page, second_page, third_page) for benchmark in page["benchmarks"]
        }
        assert returned_ids == {str(benchmark.id) for benchmark in seeded_benchmarks}

    @pytest.mark.description(
        (
            "A malformed cursor is a client error.\n"
            "\n"
            "Test cases:\n"
            "- A cursor that does not decode returns 400 instead of a server error."
        )
    )
    def test_invalid_cursor_is_rejected(self, access_key_client: TestClient) -> None:
        response = access_key_client.get(
            "/fetch-benchmarks?limit=2&cursor=garbage",
            headers={"x-api-key": "fake-key"},
        )

        assert response.status_code == 400, response.text
        assert response.json() == {"detail": "Invalid cursor"}

    @pytest.mark.description("A NUL byte in a text filter is a client error, not a Postgres failure.")
    @pytest.mark.parametrize("query", ["label=%00", "model=gpt%00", "agent_name=a%00b", "started_by=%00"])
    def test_nul_in_filter_is_rejected(self, access_key_client: TestClient, query: str) -> None:
        response = access_key_client.get(f"/fetch-benchmarks?{query}", headers={"x-api-key": "fake-key"})

        assert response.status_code == 400, response.text
        assert response.json() == {"detail": "Filters must not contain NUL characters"}

    @pytest.mark.description(
        (
            "Benchmark listing must apply status filters before pagination.\n"
            "\n"
            "Test cases:\n"
            "- A status query returns only matching benchmark rows."
        )
    )
    def test_filter_by_status(self, access_key_client: TestClient, database_session: Session) -> None:
        seeded_benchmarks = _seed_benchmarks(database_session, 4)
        response = access_key_client.get(
            "/fetch-benchmarks?status=IN_PROGRESS",
            headers={"x-api-key": "fake-key"},
        )
        assert response.status_code == 200, response.text
        response_body = response.json()

        assert {benchmark["id"] for benchmark in response_body["benchmarks"]} == {
            str(benchmark.id) for benchmark in seeded_benchmarks[::2]
        }
        assert all(benchmark["status"] == "IN_PROGRESS" for benchmark in response_body["benchmarks"])

    @pytest.mark.description(
        (
            "Benchmark listing must exclude rows older than the requested start boundary.\n"
            "\n"
            "Test cases:\n"
            "- The started-after filter returns only newer runs."
        )
    )
    def test_filter_by_started_after(self, access_key_client: TestClient, database_session: Session) -> None:
        seeded_benchmarks = _seed_benchmarks(database_session, 5)

        response = access_key_client.get(
            "/fetch-benchmarks",
            params={"started_after": "2026-01-01T00:02:00+00:00"},
            headers={"x-api-key": "fake-key"},
        )
        assert response.status_code == 200, response.text
        response_body = response.json()
        assert {benchmark["id"] for benchmark in response_body["benchmarks"]} == {
            str(benchmark.id) for benchmark in seeded_benchmarks[3:]
        }

    @pytest.mark.description(
        (
            "Existing offset clients must keep working alongside cursor pagination.\n"
            "\n"
            "Test cases:\n"
            "- Offset and limit select the expected legacy page."
        )
    )
    def test_legacy_offset_limit_still_works(
        self,
        access_key_client: TestClient,
        database_session: Session,
    ) -> None:
        _seed_benchmarks(database_session, 3)
        response = access_key_client.get(
            "/fetch-benchmarks?offset=0&limit=10",
            headers={"x-api-key": "fake-key"},
        )
        response_body = response.json()

        assert len(response_body["benchmarks"]) == 3
        assert response_body["total_count"] == 3

    @pytest.mark.description(
        (
            "Run attribution must survive the benchmark list serialization path.\n"
            "\n"
            "Test cases:\n"
            "- A persisted starter email is returned in its benchmark table row."
        )
    )
    def test_table_row_includes_started_by_email(
        self,
        access_key_client: TestClient,
        database_session: Session,
    ) -> None:
        _seed_benchmarks(database_session, 1, started_by_email="emailtest@x.com")

        response = access_key_client.get("/fetch-benchmarks", headers={"x-api-key": "fake-key"})

        response_body = response.json()
        assert response_body["benchmarks"][0]["started_by_email"] == "emailtest@x.com"

    @pytest.mark.description(
        (
            "Listing runs by project must follow each run's newest project link.\n"
            "\n"
            "Test cases:\n"
            "- A run moved to another project is listed only under the new project.\n"
            "- A run that never moved stays under its project.\n"
            "- The total count and the cursor page match the filtered rows."
        )
    )
    def test_filter_by_project_uses_newest_link(
        self,
        access_key_client: TestClient,
        database_session: Session,
        project_service: MockProjectRoutes,
    ) -> None:
        first = make_project(project_service, "first")
        second = make_project(project_service, "second")
        started = datetime(2026, 1, 1, tzinfo=timezone.utc)
        moved = make_benchmark(session=database_session)
        stayed = make_benchmark(session=database_session)
        make_benchmark(session=database_session)
        link_run_project(database_session, moved.id, first, started)
        link_run_project(database_session, moved.id, second, started + timedelta(minutes=1))
        link_run_project(database_session, stayed.id, first, started)
        database_session.commit()

        def listed(handle: str, **params: str) -> tuple[list[str], int | None]:
            response = access_key_client.get(
                "/fetch-benchmarks", params={"project": handle, **params}, headers={"x-api-key": "fake-key"}
            )
            assert response.status_code == 200, response.text
            body = response.json()
            return [row["id"] for row in body["benchmarks"]], body["total_count"]

        assert listed("second") == ([str(moved.id)], 1)
        assert listed("first") == ([str(stayed.id)], 1)
        assert listed("second", cursor="") == ([str(moved.id)], None)

    @pytest.mark.description(
        (
            "Each run row carries its newest project handle, its dataset version, and whether it ran a task subset.\n"
            "\n"
            "Test cases:\n"
            "- A run moved between projects reports the handle of the newest one.\n"
            "- A run in a hosted dataset reports its version and no subset.\n"
            "- A run with a stored benchmark service version but no hosted dataset reports no version.\n"
            "- A run outside any project with chosen task IDs or a slice reports no handle and a subset."
        )
    )
    def test_table_row_carries_project_dataset_version_and_subset(
        self,
        access_key_client: TestClient,
        database_session: Session,
        project_service: MockProjectRoutes,
    ) -> None:
        old = make_project(project_service, "old")
        project = make_project(project_service, "p")
        hosted = make_benchmark(
            session=database_session, dataset_ref=HostedDataset(benchmark="b", dataset="d", revision=3)
        )
        subset = make_benchmark(session=database_session, task_ids=["t1"])
        sliced = make_benchmark(session=database_session, slice_str="0:5")
        unhosted = make_benchmark(
            session=database_session, dataset_version=DatasetVersion(id="swe-bench-1", label=None)
        )
        linked = datetime(2026, 1, 1, tzinfo=timezone.utc)
        link_run_project(database_session, hosted.id, old, linked)
        link_run_project(database_session, hosted.id, project, linked + timedelta(minutes=1))
        database_session.commit()

        response = access_key_client.get("/fetch-benchmarks", headers={"x-api-key": "fake-key"})

        assert response.status_code == 200, response.text
        rows = {row["id"]: row for row in response.json()["benchmarks"]}
        fields = ("project_handle", "dataset_version", "task_subset")
        assert {field: rows[str(hosted.id)][field] for field in fields} == {
            "project_handle": "p",
            "dataset_version": "b/d@3",
            "task_subset": False,
        }
        assert rows[str(unhosted.id)]["dataset_version"] is None
        for unlinked in (subset, sliced):
            assert rows[str(unlinked.id)]["project_handle"] is None
            assert rows[str(unlinked.id)]["task_subset"] is True

    @pytest.mark.description(
        (
            "A regrade is a task subset only when it skipped tasks or its source run was a subset.\n"
            "\n"
            "Test cases:\n"
            "- A regrade of every task of a full run reports no subset.\n"
            "- A regrade that skipped a task reports a subset.\n"
            "- A full regrade of a subset run, and a regrade of that regrade, report a subset."
        )
    )
    def test_regrade_task_subset_follows_its_source(
        self, access_key_client: TestClient, database_session: Session
    ) -> None:
        org = database_session.get(Org, TEST_ORG_ID)
        assert org is not None
        full = make_benchmark(session=database_session)
        subset = make_benchmark(session=database_session, task_ids=["t1"])

        def regrade(source: Benchmark, skipped: list[str]) -> Benchmark:
            tasks = RegradeTasks(eval_resume_states={"t1": {}}, skipped_task_ids=skipped)
            run = regrade_run(source, tasks, RequestIdentity(org=org), dataset="d", dataset_version=None)
            database_session.add(run)
            database_session.flush()
            return run

        full_regrade = regrade(full, [])
        skipping_regrade = regrade(full, ["t2"])
        subset_regrade = regrade(subset, [])
        nested_regrade = regrade(subset_regrade, [])
        database_session.commit()

        response = access_key_client.get("/fetch-benchmarks", headers={"x-api-key": "fake-key"})

        assert response.status_code == 200, response.text
        subsets = {row["id"]: row["task_subset"] for row in response.json()["benchmarks"]}
        assert subsets[str(full_regrade.id)] is False
        assert subsets[str(skipping_regrade.id)] is True
        assert subsets[str(subset_regrade.id)] is True
        assert subsets[str(nested_regrade.id)] is True

    @pytest.mark.description(
        "An unfiltered run list still returns 200 with no project handles when the project service fails or "
        "times out, and the handle lookup runs after the transaction ends with a 2 second timeout."
    )
    @pytest.mark.parametrize("failure", [503, 500, "timeout", "malformed"])
    def test_table_rows_survive_project_service_failure(
        self,
        access_key_client: TestClient,
        database_session: Session,
        project_service: MockProjectRoutes,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        failure: int | str,
    ) -> None:
        project = make_project(project_service, "p")
        linked = make_benchmark(session=database_session)
        link_run_project(database_session, linked.id, project, datetime(2026, 1, 1, tzinfo=timezone.utc))
        database_session.commit()
        lookups: list[tuple[bool, float]] = []

        def fail(request: httpx.Request) -> httpx.Response:
            lookups.append((database_session.in_transaction(), request.extensions["timeout"]["read"]))
            if failure == "timeout":
                raise httpx.ReadTimeout("slow", request=request)
            if failure == "malformed":
                return httpx.Response(200, json={"unexpected": []})
            return httpx.Response(int(failure))

        failing = ProjectServiceClient(
            httpx.Client(base_url="http://projects", transport=UnavailableAwareTransport(httpx.MockTransport(fail)))
        )
        monkeypatch.setattr(project_repository, "project_service", lambda: failing)
        monkeypatch.setattr(results_module.logger, "handlers", [caplog.handler])

        response = access_key_client.get("/fetch-benchmarks", headers={"x-api-key": "fake-key"})

        assert response.status_code == 200, response.text
        assert [row["project_handle"] for row in response.json()["benchmarks"]] == [None]
        assert "Project handle lookup failed" in caplog.text
        assert lookups == [(False, 2)]

    @pytest.mark.description("A project handle that does not exist returns 404 on either route.")
    @pytest.mark.parametrize("path", ["/fetch-benchmarks", "/benchmarks/filter-options"])
    def test_unknown_project_returns_404(self, access_key_client: TestClient, path: str) -> None:
        response = access_key_client.get(path, params={"project": "nope"}, headers={"x-api-key": "fake-key"})

        assert response.status_code == 404, response.text
        assert response.json()["detail"] == "Project not found"

    @pytest.mark.description("A broken project lookup is not reported as an unknown project on either route.")
    @pytest.mark.parametrize("path", ["/fetch-benchmarks", "/benchmarks/filter-options"])
    def test_project_lookup_failure_is_not_a_404(
        self, access_key_client: TestClient, monkeypatch: pytest.MonkeyPatch, path: str
    ) -> None:
        def broken_lookup(self: ProjectRepository, org_id: object, handle: str) -> None:
            raise KeyError("projects")

        monkeypatch.setattr(ProjectRepository, "get", broken_lookup)

        with pytest.raises(KeyError):
            access_key_client.get(path, params={"project": "any"}, headers={"x-api-key": "fake-key"})
