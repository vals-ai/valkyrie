import logging
from datetime import datetime
from typing import Any, Sequence, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest
from benchmark_service.client import BenchmarkServiceClient, BenchmarkServiceError
from benchmark_service.schemas import VerifyTaskIdsResponse
from fastapi.testclient import TestClient
from httpx._models import Response
from sqlmodel import Session, col, func, select, update

from main import app
from executor_protocol import SUPPORTED_PROTOCOL_VERSION
from tests.factories import make_benchmark
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.runtime.secrets import SecretValue
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    FailureCategory,
    ExecutorRelease,
    Org,
    Task,
    TaskStatus,
)
from tracker.exceptions import TrackerServiceError
from tracker.executor.release_control import promote_release
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.services.runs.finalize import RUNNABLE_TASK_STATUSES
from tracker.types import FetchBenchmarksRequest, RunExecutionRequest, StartBenchmarkRequest
from tracker.services.tasks.execute import save_eval_resume_state
from tracker.services.tasks.status import commit_task_error
from tracker.services.runs.start import create_task_rows
from tracker.services.runs.finalize import fetch_final_score_inputs, set_benchmark_final_status
from tracker.services.benchmarks import fetch_benchmark_row, start_benchmark_request_to_benchmark
from tracker.clients.benchmark_service import fetch_sandbox_provider_config
from tracker.services.results import fetch_filtered_benchmark_rows


client = TestClient(app)
_ACTIVE_ATTEMPT = datetime(2026, 7, 1)


@pytest.fixture
def example_benchmark_object(contract: AgentContractRequest, database_session: Session) -> Benchmark:
    release = ExecutorRelease(
        id="test-release",
        artifact_uri="s3://artifacts/test-release.pex",
        artifact_digest="digest-test-release",
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
    )
    database_session.add(release)
    database_session.commit()
    promote_release(database_session, release.id)
    database_session.commit()

    benchmark = make_benchmark(contract=contract, concurrency=5)
    benchmark.executor_release_id = release.id
    benchmark.executor_artifact_uri = release.artifact_uri
    benchmark.executor_artifact_digest = release.artifact_digest
    benchmark.executor_protocol_version = release.protocol_version
    return benchmark


@pytest.mark.description("Benchmark state transitions, task rows, and finalization.")
class TestRunState:
    _test_org = Org(id=TEST_ORG_ID, name="default")
    _test_starter = RequestIdentity(org=_test_org)

    @pytest.mark.description(
        (
            "Sandbox provider config should combine client-selected type with the production secret shape.\n"
            "\n"
            "Test cases:\n"
            "- A selected provider type is added to DAYTONA_* secret values."
        )
    )
    async def test_fetch_sandbox_provider_config_combines_provider_type_with_secret(self) -> None:
        secrets = {
            "provider-secret": {
                "DAYTONA_API_KEY": "key",
                "DAYTONA_API_URL": "url",
                "DAYTONA_TARGET": "target",
            },
        }

        class TestSecretStore:
            async def get(self, name: str) -> SecretValue:
                return cast(SecretValue, secrets[name])

        provider_config = await fetch_sandbox_provider_config("provider-secret", TestSecretStore(), "daytona")
        assert provider_config.model_dump(mode="json") == {
            "type": "daytona",
            "DAYTONA_API_KEY": "key",
            "DAYTONA_API_URL": "url",
            "DAYTONA_TARGET": "target",
        }

    @pytest.mark.description(
        (
            "Stopping a run moves the run and its tasks to the right states.\n"
            "\n"
            "Test cases:\n"
            "- A graceful whole-run stop leaves the benchmark stopping.\n"
            "- Queued and evaluating tasks stop while in-progress tasks continue."
        )
    )
    def test_stop_benchmark(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:

        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.commit()

        initial_task_rows: list[Task] = []
        for i in range(5):
            initial_task_rows.append(
                Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.PENDING)
            )
        for i in range(5, 8):
            initial_task_rows.append(
                Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.IN_PROGRESS)
            )
        for i in range(8, 10):
            initial_task_rows.append(
                Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.EVALUATING)
            )
        database_session.add_all(initial_task_rows)
        database_session.commit()

        locked_fetches: list[bool] = []

        def capture_locked_fetch(
            benchmark_id: UUID,
            session: Session,
            org: Org,
            *,
            for_update: bool = False,
        ) -> Benchmark:
            locked_fetches.append(for_update)
            return fetch_benchmark_row(benchmark_id, session, org, for_update=for_update)

        monkeypatch.setattr("main.fetch_benchmark_row", capture_locked_fetch)

        response: Response = client.post(
            f"/stop-benchmark/{benchmark_row.id}?force=false",
        )
        assert response.status_code == 200
        assert response.json() == {"status": "success"}
        assert locked_fetches == [True]

        benchmark_row = fetch_benchmark_row(benchmark_row.id, database_session, self._test_org)
        assert benchmark_row.status == BenchmarkStatus.STOPPING

        task_rows = database_session.exec(
            select(func.count(col(Task.id)))
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.status == TaskStatus.PENDING)
        ).one()

        assert task_rows == 0

        task_rows = database_session.exec(
            select(func.count(col(Task.id)))
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.status == TaskStatus.STOPPED)
        ).one()
        assert task_rows == 7

        task_rows = database_session.exec(
            select(func.count(col(Task.id)))
            .where(Task.benchmark == benchmark_row.id)
            .where(Task.status == TaskStatus.IN_PROGRESS)
        ).one()

        assert task_rows == 3

    @pytest.mark.description("A graceful stop of a run with only in-progress tasks shows the run as stopping.")
    def test_graceful_stop_with_only_in_progress_tasks_marks_stopping(
        self, example_benchmark_object: Benchmark, database_session: Session
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        database_session.add(benchmark_row)
        database_session.add(
            Task(org_id=TEST_ORG_ID, task_id="running", benchmark=benchmark_row.id, status=TaskStatus.IN_PROGRESS)
        )
        database_session.commit()

        response: Response = client.post(f"/stop-benchmark/{benchmark_row.id}?force=false")

        assert response.status_code == 200
        database_session.expire_all()
        assert (
            fetch_benchmark_row(benchmark_row.id, database_session, self._test_org).status == BenchmarkStatus.STOPPING
        )
        task = database_session.exec(select(Task).where(col(Task.benchmark) == benchmark_row.id)).one()
        assert task.status == TaskStatus.IN_PROGRESS

    @pytest.mark.description(
        (
            "A run that can't be stopped returns an error.\n"
            "\n"
            "Test cases:\n"
            "- Cannot stop a benchmark that is not in progress.\n"
            "- Cannot stop a benchmark where all tasks have already started.\n"
            "- Errors are raised and returned to the client."
        )
    )
    def test_stop_benchmark_edge_cases(self, example_benchmark_object: Benchmark, database_session: Session) -> None:

        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.FINISHED
        database_session.add(benchmark_row)
        database_session.commit()

        response: Response = client.post(f"/stop-benchmark/{benchmark_row.id}?force=false")
        assert response.status_code == 400

    @pytest.mark.description(
        (
            "Resuming a run sets it in progress and moves only its stopped and errored tasks back to pending.\n"
            "\n"
            "Test cases:\n"
            "- A stopped run with a stopped task can be resumed.\n"
            "- After resuming, the run is in progress and its stopped tasks are pending.\n"
            "- Only stopped tasks change status.\n"
            "- A run with errored tasks can be resumed."
        )
    )
    def test_resume_benchmark(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
    ) -> None:

        benchmark_row = example_benchmark_object
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        task_rows: list[Task] = []
        for i in range(5):
            task_rows.append(
                Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.STOPPED)
            )
        for i in range(5, 10):
            task_rows.append(
                Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.FINISHED)
            )
        database_session.add_all(task_rows)
        database_session.commit()

        task_ids = database_session.exec(
            select(Task.task_id).where(Task.benchmark == benchmark_row.id).where(Task.status == TaskStatus.STOPPED)
        ).all()

        assert len(task_ids) == 5

        response: Response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false",
        )
        assert response.status_code == 200, response.text
        assert response.json() == {"status": "success"}

        task_ids = database_session.exec(
            select(Task.task_id).where(Task.benchmark == benchmark_row.id).where(Task.status == TaskStatus.PENDING)
        ).all()
        assert len(task_ids) == 5

        benchmark_row = fetch_benchmark_row(benchmark_row.id, database_session, self._test_org)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS

        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        fetched_task_rows: Sequence[Task] = database_session.exec(
            select(Task).where(col(Task.benchmark) == benchmark_row.id)
        ).all()
        assert len(fetched_task_rows) == 10

        for i, task_row in enumerate(fetched_task_rows):
            if i < 5:
                task_row.status = TaskStatus.ERROR
            else:
                task_row.status = TaskStatus.STOPPED

        database_session.commit()

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=true",
        )
        assert response.status_code == 200
        assert response.json() == {"status": "success"}

        fetched_task_rows = database_session.exec(select(Task).where(col(Task.benchmark) == benchmark_row.id)).all()
        assert len(fetched_task_rows) == 10
        assert all(task_row.status == TaskStatus.PENDING for task_row in fetched_task_rows)

    @pytest.mark.description(
        (
            "Resuming a run that can't be resumed is rejected, or does nothing when there is nothing to retry.\n"
            "\n"
            "Test cases:\n"
            "- Running benchmark retry with no error tasks is a no-op.\n"
            "- Cannot resume a benchmark where all tasks have already finished.\n"
            "- Downstream errors return a stable generic client detail.\n"
            "- Can recreate the same environment the benchmark was started in.\n"
            "- Can force resume a task and validate the task ids passed in."
        )
    )
    def test_resume_benchmark_edge_cases(
        self,
        contract: AgentContractRequest,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:

        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        response: Response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false",
        )
        assert response.status_code == 200

        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        task_rows = [
            Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.FINISHED)
            for i in range(5)
        ]
        database_session.add_all(task_rows)
        database_session.commit()

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false",
        )
        assert response.status_code == 200

        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS

        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        original_start_benchmark_request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=5,
            task_ids=["task_0", "task_1", "task_2", "task_3", "task_4"],
            slice_str=":10",
            sandbox_provider="modal",
            sandbox_provider_secret_name="ModalSecrets",
        )

        benchmark_row = start_benchmark_request_to_benchmark(
            original_start_benchmark_request,
            self._test_starter,
        )
        assert benchmark_row.arguments.sandbox_provider_secret_name == "ModalSecrets"

        recreated_start_benchmark_request = benchmark_row.managed_start_benchmark_request()
        assert recreated_start_benchmark_request == RunExecutionRequest.model_validate(
            original_start_benchmark_request.model_dump(mode="python")
        )

        task_rows = database_session.exec(select(Task).where(col(Task.benchmark) == example_benchmark_object.id)).all()
        assert len(task_rows) == 5

        database_session.exec(
            update(Task).where(col(Task.task_id) == task_rows[0].task_id).values(status=TaskStatus.STOPPED)
        )
        database_session.commit()

        async def _verify_rejecting_task_5(
            *_args: Any,
            task_ids: list[str] | None,
            **_kwargs: Any,
        ) -> VerifyTaskIdsResponse:
            if task_ids and "task_5" in task_ids:
                raise BenchmarkServiceError(
                    'Verify task ids failed with status code 400, response: {"detail":"Task ID not found: task_5"}',
                    status_code=400,
                    detail="Task ID not found: task_5",
                )
            return VerifyTaskIdsResponse(task_ids=task_ids or [])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_rejecting_task_5)

        response = client.post(
            f"/retry-or-resume-benchmark/{example_benchmark_object.id}?retry=false",
            json={"task_ids": ["task_5"]},
        )
        assert response.status_code == 400
        assert response.json() == {"detail": "Task ID not found: task_5"}

        task_rows = database_session.exec(
            select(Task).where(
                (col(Task.benchmark) == example_benchmark_object.id) & (col(Task.task_id) != task_rows[0].task_id)
            )
        ).all()
        task_ids = [task_row.task_id for task_row in task_rows]

        assert all(task_row.status == TaskStatus.FINISHED for task_row in task_rows)

        response = client.post(
            f"/retry-or-resume-benchmark/{example_benchmark_object.id}?retry=false",
            json={"task_ids": task_ids},
        )
        assert response.status_code == 200
        assert response.json() == {"status": "success"}

        task_rows = database_session.exec(select(Task).where(col(Task.benchmark) == example_benchmark_object.id)).all()
        assert len(task_rows) == 5
        assert all(task_row.status == TaskStatus.PENDING for task_row in task_rows)

    @pytest.mark.description("A running run rejects secret overrides unless it is retried.")
    def test_running_benchmark_rejects_secret_overrides_without_retry(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()
        original_arguments = benchmark_row.arguments.model_copy(deep=True)
        enqueue = AsyncMock()
        monkeypatch.setattr("main._launch_executor_dispatch", enqueue)

        response = client.post(
            f"/retry-or-resume-benchmark/{benchmark_row.id}?retry=false",
            json={"secrets": {"MODEL_API_KEY": "replacement"}},
        )

        assert response.status_code == 409
        assert response.json() == {"detail": "Secret overrides require retry=true while a run is in progress."}
        database_session.refresh(benchmark_row)
        assert benchmark_row.status == BenchmarkStatus.IN_PROGRESS
        assert benchmark_row.arguments == original_arguments
        enqueue.assert_not_awaited()

    @pytest.mark.description(
        (
            "Creating task rows adds only the missing tasks, all pending.\n"
            "\n"
            "Test cases:\n"
            "- No tasks exist in the database already.\n"
            "- Some tasks exist in the database already.\n"
            "- No duplicate tasks are created.\n"
            "- All returned tasks are in the pending state."
        )
    )
    def test_create_task_rows(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        executor_authority: Any,
    ) -> None:

        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)

        verified_task_ids = [f"task_{i}" for i in range(5)]

        task_rows = create_task_rows(
            verified_task_ids, benchmark_row, database_session, self._test_org, authority=authority
        )
        assert len(task_rows) == len(verified_task_ids)
        assert all(task_row[1].status == TaskStatus.PENDING for task_row in task_rows)

        for i, task_row in enumerate(task_rows):
            assert task_row[0] == verified_task_ids[i]

        task_rows = create_task_rows(
            verified_task_ids, benchmark_row, database_session, self._test_org, authority=authority
        )
        assert len(task_rows) == len(verified_task_ids)
        assert all(task_row[1].status == TaskStatus.PENDING for task_row in task_rows)

        all_tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        assert len(all_tasks) == len(verified_task_ids)
        assert all(task.status == TaskStatus.PENDING for task in all_tasks)

        task_rows = create_task_rows(["task_1"], benchmark_row, database_session, self._test_org, authority=authority)
        assert [task_id for task_id, _ in task_rows] == ["task_1"]

    @pytest.mark.description("Final score inputs wait for runnable tasks.")
    def test_fetch_final_score_inputs_waits_for_runnable_tasks(
        self, example_benchmark_object: Benchmark, database_session: Session
    ) -> None:
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        finished_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_finished",
            benchmark=benchmark_row.id,
            status=TaskStatus.FINISHED,
        )
        error_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_error",
            benchmark=benchmark_row.id,
            status=TaskStatus.ERROR,
        )
        stopped_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_stopped",
            benchmark=benchmark_row.id,
            status=TaskStatus.STOPPED,
        )
        pending_task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_pending",
            benchmark=benchmark_row.id,
            status=TaskStatus.PENDING,
        )
        database_session.add_all([finished_task, error_task, stopped_task, pending_task])
        database_session.commit()

        database_session.add(EvaluationResult(org_id=TEST_ORG_ID, task=finished_task.id, result={"score": 1.0}))
        database_session.commit()

        assert BenchmarkRepository(database_session).has_tasks(TEST_ORG_ID, benchmark_row.id, RUNNABLE_TASK_STATUSES)

        pending_task.status = TaskStatus.ERROR
        database_session.add(pending_task)
        database_session.commit()

        assert not BenchmarkRepository(database_session).has_tasks(
            TEST_ORG_ID, benchmark_row.id, RUNNABLE_TASK_STATUSES
        )
        assert fetch_final_score_inputs(database_session, benchmark_row, self._test_org) == {
            "task_finished": {"score": 1.0},
            "task_error": None,
            "task_stopped": None,
            "task_pending": None,
        }

    @pytest.mark.description("Committing a task error records a span for the status change.")
    def test_commit_task_error_spans_status_transition(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        executor_authority: Any,
        caplog: pytest.LogCaptureFixture,
        request: pytest.FixtureRequest,
    ) -> None:
        span_records: list[dict[str, Any]] = []

        class MockSpan:
            def __init__(self, record: dict[str, Any]) -> None:
                self._record = record

            def __enter__(self) -> "MockSpan":
                self._record["entered"] = True
                return self

            def __exit__(self, *_args: object) -> None:
                self._record["exited"] = True

        def fake_span(message: str, **attributes: Any) -> MockSpan:
            record = {"message": message, **attributes}
            span_records.append(record)
            return MockSpan(record)

        monkeypatch.setattr("tracker.observability.tracing.logfire.span", fake_span)
        tasks_logger = logging.getLogger("tracker.services.tasks")
        caplog.set_level(logging.INFO, logger=tasks_logger.name)
        tasks_logger.addHandler(caplog.handler)
        request.addfinalizer(lambda: tasks_logger.removeHandler(caplog.handler))

        database_session.add(example_benchmark_object)
        database_session.commit()
        authority = executor_authority(example_benchmark_object, session=database_session)

        task_row = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=example_benchmark_object.id,
            status=TaskStatus.IN_PROGRESS,
        )
        database_session.add(example_benchmark_object)
        database_session.add(task_row)
        database_session.commit()

        commit_task_error(
            task_row,
            database_session,
            "agent failed",
            producer="tracker",
            operation="process_task",
            error_type="RuntimeError",
            category=FailureCategory.UNKNOWN,
            authority=authority,
        )

        database_session.refresh(task_row)
        assert task_row.status == TaskStatus.ERROR
        error_result = database_session.exec(
            select(ErrorResult).where(ErrorResult.task == task_row.id).where(ErrorResult.org_id == TEST_ORG_ID)
        ).one()
        assert error_result.error_message == "agent failed"
        assert error_result.producer == "tracker"
        assert error_result.operation == "process_task"
        assert error_result.error_type == "RuntimeError"
        assert error_result.cause_code is None
        assert error_result.retry_scheduled is False
        assert error_result.failed_attempt_number is None
        transition_record = next(record for record in span_records if record["message"] == "task.status_transition")
        assert transition_record["from_status"] == TaskStatus.IN_PROGRESS.value
        assert transition_record["to_status"] == TaskStatus.ERROR.value
        assert transition_record["task_id"] == "task_0"
        assert transition_record["benchmark_id"] == str(example_benchmark_object.id)
        assert transition_record["entered"] and transition_record["exited"]
        assert transition_record["has_error_message"]
        assert not any(record.getMessage().startswith("task.status_transition") for record in caplog.records)

    @pytest.mark.description("Committing a task error leaves finished tasks alone.")
    @pytest.mark.parametrize("status", [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED])
    def test_commit_task_error_preserves_terminal_tasks(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        executor_authority: Any,
        status: TaskStatus,
    ) -> None:
        task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=example_benchmark_object.id,
            status=status,
        )
        database_session.add_all([example_benchmark_object, task])
        database_session.commit()
        authority = executor_authority(example_benchmark_object, session=database_session)

        assert not commit_task_error(
            task,
            database_session,
            "stale failure",
            producer="tracker",
            operation="process_task",
            error_type="RuntimeError",
            category=FailureCategory.UNKNOWN,
            authority=authority,
        )

        database_session.refresh(task)
        assert task.status == status
        assert database_session.exec(select(ErrorResult.id).where(ErrorResult.task == task.id)).first() is None

    @pytest.mark.description("Eval resume state rejects rows that are not active.")
    @pytest.mark.parametrize(
        "status",
        [TaskStatus.PENDING, TaskStatus.ERROR, TaskStatus.FINISHED, TaskStatus.STOPPED, TaskStatus.EVALUATING],
    )
    def test_eval_resume_state_rejects_non_active_rows(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        executor_authority: Any,
        monkeypatch: pytest.MonkeyPatch,
        status: TaskStatus,
    ) -> None:
        monkeypatch.setattr("tracker.services.tasks.execute.engine", database_session.bind)
        monkeypatch.setattr("tracker.services.tasks.monitor.engine", database_session.bind)
        task_row = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=example_benchmark_object.id,
            status=status,
            started_at=datetime(2026, 6, 30) if status == TaskStatus.EVALUATING else _ACTIVE_ATTEMPT,
        )
        database_session.add_all([example_benchmark_object, task_row])
        database_session.commit()
        authority = executor_authority(example_benchmark_object, session=database_session)

        updated = save_eval_resume_state(
            task_row.id,
            self._test_org,
            {"cursor": "next"},
            expected_started_at=_ACTIVE_ATTEMPT,
            authority=authority,
        )

        database_session.refresh(task_row)
        assert not updated
        assert task_row.eval_resume_state is None

    @pytest.mark.description("Committing a task error rolls back when its start time is stale.")
    def test_commit_task_error_rolls_back_when_started_at_is_stale(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        executor_authority: Any,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()
        authority = executor_authority(example_benchmark_object, session=database_session)
        task_row = Task(
            org_id=TEST_ORG_ID,
            task_id="stale-task",
            benchmark=example_benchmark_object.id,
            status=TaskStatus.IN_PROGRESS,
        )
        database_session.add(task_row)
        database_session.commit()

        committed = commit_task_error(
            task_row,
            database_session,
            "stale failure",
            producer="tracker",
            operation="process_task",
            error_type="RuntimeError",
            category=FailureCategory.UNKNOWN,
            expected_started_at=datetime(2000, 1, 1, tzinfo=ZoneInfo("UTC")),
            authority=authority,
        )

        assert committed is False
        database_session.expire_all()
        persisted_task = database_session.get(Task, task_row.id)
        assert persisted_task is not None
        assert persisted_task.status == TaskStatus.IN_PROGRESS
        assert persisted_task.finished_at is None
        assert database_session.exec(select(ErrorResult).where(ErrorResult.task == task_row.id)).all() == []

    @pytest.mark.description(
        (
            "A run's final status follows its tasks. It is finished when all finished and stopped when any stopped.\n"
            "\n"
            "Test cases:\n"
            "- Error is raised if tasks are still in the pending or in progress state.\n"
            "- Benchmark status is set to finished if all tasks are finished.\n"
            "- Benchmark status is set to stopped if any tasks are stopped."
        )
    )
    async def test_set_benchmark_final_status(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        executor_authority: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:

        finalized_statuses: list[str] = []

        def record_span(name: str, **attributes: Any) -> MagicMock:
            if name == "run.finalized":
                finalized_statuses.append(attributes["status"])
            return MagicMock()

        monkeypatch.setattr("tracker.services.runs.start.observability_span", record_span)
        monkeypatch.setattr("tracker.services.runs.finalize.observability_span", record_span)

        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()
        authority = executor_authority(benchmark_row, session=database_session)

        task_ids = [f"task_{i}" for i in range(5)]
        task_rows = create_task_rows(task_ids, benchmark_row, database_session, self._test_org, authority=authority)
        assert len(task_rows) == len(task_ids)
        assert all(task_row[1].status == TaskStatus.PENDING for task_row in task_rows)

        with pytest.raises(TrackerServiceError):
            set_benchmark_final_status(benchmark_row, database_session, self._test_org, authority=authority)

        database_session.exec(
            update(Task)
            .where(col(Task.benchmark) == benchmark_row.id)
            .values(status=TaskStatus.FINISHED, finished_at=datetime.now(ZoneInfo("UTC")))
        )
        database_session.commit()

        set_benchmark_final_status(benchmark_row, database_session, self._test_org, authority=authority)
        database_session.refresh(benchmark_row, attribute_names=["status"])
        assert benchmark_row.status == BenchmarkStatus.FINISHED

        benchmark_row.status = BenchmarkStatus.IN_PROGRESS
        benchmark_row.finished_at = None
        database_session.add(benchmark_row)
        database_session.commit()

        stopped_tasks = task_ids[:2]
        database_session.exec(
            update(Task)
            .where(col(Task.task_id).in_(stopped_tasks))
            .values(status=TaskStatus.STOPPED, finished_at=datetime.now(ZoneInfo("UTC")))
        )
        database_session.commit()

        set_benchmark_final_status(benchmark_row, database_session, self._test_org, authority=authority)
        database_session.refresh(benchmark_row, attribute_names=["status", "finished_at"])
        assert benchmark_row.status == BenchmarkStatus.STOPPED
        assert benchmark_row.finished_at is not None
        assert finalized_statuses == ["FINISHED", "STOPPED"]


@pytest.mark.description("Run filtering by starter email.")
class TestFetchStartedByFilter:
    @pytest.mark.description("Runs filter by a single starter.")
    def test_fetch_filtered_started_by_single(self, database_session: Session) -> None:
        org = database_session.get(Org, TEST_ORG_ID)
        assert org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)
        make_benchmark(started_by_email="bob@vals.ai", session=database_session)
        make_benchmark(started_by_email=None, session=database_session)

        rows, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=["alice@vals.ai"], limit=10),
            database_session,
            org,
        )
        assert total == 1
        assert [r.started_by_email for r in rows] == ["alice@vals.ai"]

    @pytest.mark.description("Runs filter by several starters.")
    def test_fetch_filtered_started_by_multiple(self, database_session: Session) -> None:
        org = database_session.get(Org, TEST_ORG_ID)
        assert org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)
        make_benchmark(started_by_email="bob@vals.ai", session=database_session)
        make_benchmark(started_by_email="carol@vals.ai", session=database_session)

        rows, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=["alice@vals.ai", "bob@vals.ai"], limit=10),
            database_session,
            org,
        )
        assert total == 2
        assert sorted(email for r in rows if isinstance((email := r.started_by_email), str)) == [
            "alice@vals.ai",
            "bob@vals.ai",
        ]

    @pytest.mark.description("Uppercase input matches the lower-cased rows in the DB.")
    def test_fetch_filtered_started_by_case_insensitive(self, database_session: Session) -> None:
        org = database_session.get(Org, TEST_ORG_ID)
        assert org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)

        rows, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=["ALICE@VALS.AI"], limit=10),
            database_session,
            org,
        )
        assert total == 1
        assert rows[0].started_by_email == "alice@vals.ai"

    @pytest.mark.description("No starter filter returns every run.")
    def test_fetch_filtered_started_by_none_skips_filter(self, database_session: Session) -> None:
        org = database_session.get(Org, TEST_ORG_ID)
        assert org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)
        make_benchmark(started_by_email=None, session=database_session)

        _, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=None, limit=10),
            database_session,
            org,
        )
        assert total == 2

    @pytest.mark.description("Trailing whitespace on a filter email matches the clean DB row.")
    def test_fetch_filtered_started_by_strips_whitespace(self, database_session: Session) -> None:
        org = database_session.get(Org, TEST_ORG_ID)
        assert org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)

        rows, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=["  alice@vals.ai  "], limit=10),
            database_session,
            org,
        )
        assert total == 1
        assert rows[0].started_by_email == "alice@vals.ai"

    @pytest.mark.description(
        "The filter applies on top of the organization scope, so rows from other organizations never leak."
    )
    def test_fetch_filtered_started_by_does_not_leak_across_orgs(self, database_session: Session) -> None:

        other_org = Org(id=uuid4(), name="other-tenant")
        database_session.add(other_org)
        database_session.commit()

        make_benchmark(
            org_id=other_org.id,
            started_by_email="alice@vals.ai",
            started_by_id="K-other",
            session=database_session,
        )

        default_org = database_session.get(Org, TEST_ORG_ID)
        assert default_org is not None
        make_benchmark(started_by_email="alice@vals.ai", session=database_session)

        rows, total, _ = fetch_filtered_benchmark_rows(
            FetchBenchmarksRequest(started_by=["alice@vals.ai"], limit=10),
            database_session,
            default_org,
        )
        assert total == 1
        assert rows[0].org_id == TEST_ORG_ID


@pytest.mark.description(
    (
        "Label filtering should ignore case while preserving stored label casing.\n"
        "\n"
        "Test cases:\n"
        "- A mixed-case label returns when queried with different casing.\n"
        "- Unlabeled and differently labeled runs are excluded."
    )
)
def test_fetch_filtered_by_label(database_session: Session) -> None:
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    make_benchmark(started_by_email="alice@vals.ai", label="Nightly", session=database_session)
    make_benchmark(started_by_email="bob@vals.ai", label="manual", session=database_session)
    make_benchmark(started_by_email="carol@vals.ai", label=None, session=database_session)

    rows, total, _ = fetch_filtered_benchmark_rows(
        FetchBenchmarksRequest(label="nightLY", limit=10),
        database_session,
        org,
    )

    assert total == 1
    assert rows[0].label == "Nightly"
