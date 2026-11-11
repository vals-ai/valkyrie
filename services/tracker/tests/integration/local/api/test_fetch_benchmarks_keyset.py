import pytest

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import (
    Benchmark,
)
from tracker.domain import BenchmarkStatus


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
