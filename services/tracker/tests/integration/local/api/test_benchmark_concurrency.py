import pytest

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import Benchmark
from tracker.domain import BenchmarkStatus


@pytest.mark.description("An authenticated update must return and persist the requested active-run limit.")
def test_authenticated_concurrency_update_persists_full_arguments(
    client: TestClient,
    database_session: Session,
) -> None:
    benchmark = make_benchmark(concurrency=2, session=database_session)
    original_arguments = benchmark.arguments

    response = client.patch(
        f"/benchmarks/{benchmark.id}/concurrency",
        headers={"X-Api-Key": "fake"},
        json={"concurrency": 4},
    )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "benchmark_id": str(benchmark.id),
        "status": BenchmarkStatus.IN_PROGRESS,
        "concurrency": 4,
    }

    database_session.expire_all()
    stored_run = database_session.get(Benchmark, benchmark.id)
    assert stored_run is not None
    assert stored_run.arguments == original_arguments.model_copy(update={"concurrency": 4})
