from unittest.mock import AsyncMock
from uuid import UUID, uuid4

from starlette.testclient import TestClient
import pytest
from sqlmodel import Session

from main import app
from tests.utils import TEST_ORG_ID
from tracker.api.parsing import parse_csv
from tracker.aws.s3 import S3ObjectStore
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    AWSBenchmarkArguments,
    BenchmarkStatus,
    Task,
    TaskStatus,
)

_client = TestClient(app)
_RESULTS_RUN_ID = UUID("22222222-2222-4222-8222-222222222222")


@pytest.mark.description("Benchmark status polling and result-existence queries.")
class TestBenchmarkStatusQueries:
    @pytest.mark.description(
        (
            "An empty status filter must be a successful no-op for polling clients.\n"
            "\n"
            "Test cases:\n"
            "- Empty CSV input and the route both return empty results."
        )
    )
    def test_benchmarks_status_empty_ids_returns_empty_entries(self) -> None:
        assert parse_csv(" , ", UUID) == []

        response = _client.get("/benchmarks/status?ids=")

        assert response.status_code == 200
        assert response.json() == {"entries": []}

    @pytest.mark.description(
        (
            "Unknown run IDs must not create placeholder status entries.\n"
            "\n"
            "Test cases:\n"
            "- A valid but missing UUID returns an empty list."
        )
    )
    def test_benchmarks_status_unknown_id_returns_empty_entries(self) -> None:
        response = _client.get(f"/benchmarks/status?ids={uuid4()}")

        assert response.status_code == 200
        assert response.json() == {"entries": []}

    @pytest.mark.description(
        (
            "Malformed run IDs must be rejected instead of silently dropped.\n"
            "\n"
            "Test cases:\n"
            "- A non-UUID entry, alone or next to a valid UUID, returns 422."
        )
    )
    def test_benchmarks_status_malformed_id_returns_422(self) -> None:
        for ids in ("not-a-uuid", f"{uuid4()},not-a-uuid"):
            response = _client.get(f"/benchmarks/status?ids={ids}")

            assert response.status_code == 422
            assert response.json() == {"detail": "ids must be a comma-separated list of run UUIDs"}

    @pytest.mark.description("Result existence must check the canonical results S3 key.")
    def test_results_exist_checks_results_key(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        example_benchmark_object.id = _RESULTS_RUN_ID
        database_session.add(example_benchmark_object)
        database_session.commit()
        exists_mock = AsyncMock(return_value=True)
        monkeypatch.setattr(S3ObjectStore, "exists", exists_mock)

        response = _client.get(f"/check-results-exist?benchmark_id={_RESULTS_RUN_ID}")

        assert response.status_code == 200
        assert response.json() == {"exists": True}
        assert exists_mock.await_args is not None
        assert exists_mock.await_args.args[0] == f"benchmarks/{_RESULTS_RUN_ID}/{example_benchmark_object.name}.json"

    @pytest.mark.description(
        (
            "Status polling must count stopped tasks as complete alongside finished and errored tasks.\n"
            "\n"
            "Test cases:\n"
            "- Three terminal states contribute to finished progress while an active task does not.\n"
            "- Invalid UUID tokens are ignored without hiding the valid benchmark."
        )
    )
    def test_benchmarks_status_counts_all_terminal_tasks(
        self,
        database_session: Session,
        contract: AgentContractRequest,
    ) -> None:
        benchmark_id = uuid4()
        database_session.add(
            Benchmark(
                org_id=TEST_ORG_ID,
                id=benchmark_id,
                name="swebench",
                status=BenchmarkStatus.IN_PROGRESS,
                arguments=AWSBenchmarkArguments(contract=contract, concurrency=1),
            )
        )
        database_session.add_all(
            [
                Task(org_id=TEST_ORG_ID, task_id="finished", benchmark=benchmark_id, status=TaskStatus.FINISHED),
                Task(org_id=TEST_ORG_ID, task_id="error", benchmark=benchmark_id, status=TaskStatus.ERROR),
                Task(org_id=TEST_ORG_ID, task_id="stopped", benchmark=benchmark_id, status=TaskStatus.STOPPED),
                Task(org_id=TEST_ORG_ID, task_id="running", benchmark=benchmark_id, status=TaskStatus.IN_PROGRESS),
            ]
        )
        database_session.commit()

        response = _client.get(f"/benchmarks/status?ids={benchmark_id}")

        assert response.status_code == 200
        assert response.json()["entries"] == [
            {
                "id": str(benchmark_id),
                "status": "IN_PROGRESS",
                "finished_at": None,
                "executor_release_id": None,
                "current_execution_release_id": None,
                "executor_artifact_digest": None,
                "executor_protocol_version": None,
                "total_tasks": 4,
                "finished_tasks": 3,
                "task_state_counts": {"FINISHED": 1, "ERROR": 1, "STOPPED": 1, "IN_PROGRESS": 1},
            }
        ]
