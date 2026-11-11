import pytest

from uuid import uuid4

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import ExecutorRelease
from tracker.domain import BenchmarkStatus


@pytest.mark.description("Bulk benchmark status responses, scoping, and authentication.")
class TestBenchmarksStatus:
    @pytest.mark.description(
        (
            "Status polling must return current counts for each requested run.\n"
            "\n"
            "Test cases:\n"
            "- An authenticated request receives entries for its requested benchmark IDs."
        )
    )
    def test_status_returns_requested_ids(self, client: TestClient, database_session: Session) -> None:
        release = ExecutorRelease(
            id="status-release",
            artifact_uri="source:///status-release",
            artifact_digest="a" * 64,
            protocol_version="1",
        )
        running_benchmark = make_benchmark("running", status=BenchmarkStatus.IN_PROGRESS)
        running_benchmark.current_execution_release_id = release.id
        finished_benchmark = make_benchmark("finished", status=BenchmarkStatus.FINISHED)
        database_session.add_all([release, running_benchmark, finished_benchmark])
        database_session.commit()

        response = client.get(
            f"/benchmarks/status?ids={running_benchmark.id},{finished_benchmark.id}",
            headers={"X-Api-Key": "fake"},
        )

        assert response.status_code == 200, response.text
        response_body = response.json()
        entries = {entry["id"]: entry for entry in response_body["entries"]}
        assert set(entries) == {str(running_benchmark.id), str(finished_benchmark.id)}
        assert entries[str(running_benchmark.id)]["current_execution_release_id"] == release.id
        assert entries[str(finished_benchmark.id)]["current_execution_release_id"] is None

    @pytest.mark.description(
        (
            "Status polling must not reveal runs from another organization.\n"
            "\n"
            "Test cases:\n"
            "- Requested foreign benchmark IDs are omitted from the response."
        )
    )
    def test_status_ignores_foreign_ids(self, client: TestClient, database_session: Session) -> None:
        benchmark = make_benchmark("own", status=BenchmarkStatus.IN_PROGRESS)
        database_session.add(benchmark)
        database_session.commit()
        foreign_benchmark_id = uuid4()

        response = client.get(
            f"/benchmarks/status?ids={benchmark.id},{foreign_benchmark_id}",
            headers={"X-Api-Key": "fake"},
        )

        response_body = response.json()
        assert len(response_body["entries"]) == 1
        assert response_body["entries"][0]["id"] == str(benchmark.id)

    @pytest.mark.description(
        (
            "An empty status request must be a successful no-op.\n"
            "\n"
            "Test cases:\n"
            "- Omitting benchmark IDs returns an empty entries list."
        )
    )
    def test_status_no_ids_returns_empty(self, client: TestClient) -> None:
        response = client.get("/benchmarks/status", headers={"X-Api-Key": "fake"})

        assert response.status_code == 200
        assert response.json() == {"entries": []}

    @pytest.mark.description(
        (
            "Benchmark status polling must require authentication.\n"
            "\n"
            "Test cases:\n"
            "- A request without a bearer session receives 401."
        )
    )
    def test_status_unauthenticated_returns_401(self, client: TestClient) -> None:
        response = client.get("/benchmarks/status?ids=00000000-0000-0000-0000-000000000001")

        assert response.status_code == 401
