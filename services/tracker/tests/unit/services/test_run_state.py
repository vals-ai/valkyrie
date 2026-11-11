from datetime import datetime
from typing import Any, Sequence, cast
from unittest.mock import AsyncMock
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
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.runtime.secrets import SecretValue
from tracker.database.models import (
    Benchmark,
    ErrorResult,
    EvaluationResult,
    ExecutorRelease,
    Org,
    Task,
)
from tracker.types import AgentContractRequest
from tracker.database.models import BenchmarkDefinition, Dataset, RunCost
from tracker.search_cost import SearchCharge
from decimal import Decimal
from tracker.repositories.dataset_version import DatasetVersionRepository
from tracker.services.datasets.ref import HostedDataset
from tracker.domain import BenchmarkStatus, FailureCategory, TaskStatus
from tracker.executor.control_tasks import (
    TaskControl,
    TaskDecision,
    TaskFailure,
    TaskFailureRequest,
    TaskResumeRequest,
    TaskSearchCostsRequest,
)
from tracker.executor.control_protocol import AttemptRef, PhaseRef
from tracker.executor.control_runs import RunControl
from tracker.executor.release_control import promote_release

from tracker.types import FetchBenchmarksRequest, RunExecutionRequest, StartBenchmarkRequest
from tracker.services.results import fetch_filtered_benchmark_rows
from tracker.services.benchmarks import fetch_benchmark_row, start_benchmark_request_to_benchmark
from tracker.clients.benchmark_service import fetch_sandbox_provider_config


client = TestClient(app)
_ACTIVE_ATTEMPT = datetime(2026, 7, 1)


def _task_failure(message: str) -> TaskFailure:
    return TaskFailure(
        message=message,
        producer="tracker",
        operation="process_task",
        error_type="RuntimeError",
        category=FailureCategory.UNKNOWN,
        cause_code=None,
        phase=None,
        transient=None,
    )


@pytest.fixture
def example_benchmark_object(contract: AgentContractRequest, database_session: Session) -> Benchmark:
    release = ExecutorRelease(
        id="test-release",
        artifact_uri="source:///test-release",
        artifact_digest="a" * 64,
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
        "Run state creates missing pending task rows once and returns all tasks on subsequent reads."
    )
    async def test_read_run_state_creates_missing_task_rows(
        self, contract: AgentContractRequest, control_postgres_session: Session, bound_controls: Any
    ) -> None:
        benchmark = make_benchmark(contract=contract, concurrency=5)
        control_postgres_session.add(benchmark)
        control_postgres_session.commit()
        run_control = cast(RunControl, bound_controls(benchmark, session=control_postgres_session)["run_control"])
        task_ids = [f"task_{i}" for i in range(5)]

        snapshot = await run_control.read_run_state(task_ids, create_missing_tasks=True)
        assert {task.ref.task_id: task.status for task in snapshot.tasks} == dict.fromkeys(task_ids, TaskStatus.PENDING)
        assert snapshot.authority_current

        repeated = await run_control.read_run_state(task_ids, create_missing_tasks=True)
        assert [(task.ref.task_id, task.ref.task_row_id) for task in repeated.tasks] == [
            (task.ref.task_id, task.ref.task_row_id) for task in snapshot.tasks
        ]
        rows = control_postgres_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
        assert len(rows) == len(task_ids)
        assert all(task.status == TaskStatus.PENDING for task in rows)
        selected = await run_control.read_run_state(["task_1"], create_missing_tasks=True)
        assert len(selected.tasks) == len(task_ids)
        assert {task.ref.task_id for task in selected.tasks} == set(task_ids)

    @pytest.mark.description(
        "A run pinned to a hosted dataset version reads that version's task and metric script digests."
    )
    async def test_read_dataset_version_returns_the_pinned_versions_digests(
        self, contract: AgentContractRequest, control_postgres_session: Session, bound_controls: Any
    ) -> None:
        definition = BenchmarkDefinition(org_id=TEST_ORG_ID, name="hosted")
        control_postgres_session.add(definition)
        control_postgres_session.flush()
        dataset = Dataset(benchmark_definition_id=definition.id, name="smoke")
        control_postgres_session.add(dataset)
        control_postgres_session.flush()
        repository = DatasetVersionRepository(control_postgres_session)
        first, _ = repository.publish(dataset.id, {"a": ("1" * 64, 1)}, [], None, metric_digest="m" * 64)
        repository.publish(dataset.id, {"a": ("2" * 64, 1), "b": ("3" * 64, 1)}, [], None)
        benchmark = make_benchmark(contract=contract, concurrency=1)
        ref = HostedDataset(benchmark="hosted", dataset="smoke", dataset_id=dataset.id, revision=first.revision)
        benchmark.arguments = benchmark.arguments.model_copy(update={"dataset_ref": ref})
        control_postgres_session.add(benchmark)
        control_postgres_session.commit()
        run_control = cast(RunControl, bound_controls(benchmark, session=control_postgres_session)["run_control"])

        snapshot = await run_control.read_dataset_version()

        assert snapshot.task_digests == {"a": "1" * 64}
        assert snapshot.metric_digest == "m" * 64

    @pytest.mark.description(
        "Finalization waits for runnable tasks, then chooses stopped or score inputs from terminal tasks."
    )
    async def test_prepare_finalization_waits_for_runnable_tasks(
        self, contract: AgentContractRequest, control_postgres_session: Session, bound_controls: Any
    ) -> None:
        benchmark = make_benchmark(contract=contract, concurrency=5)
        control_postgres_session.add(benchmark)
        control_postgres_session.commit()
        run_control = cast(RunControl, bound_controls(benchmark, session=control_postgres_session)["run_control"])
        tasks = [
            Task(org_id=TEST_ORG_ID, task_id=task_id, benchmark=benchmark.id, status=status, started_at=_ACTIVE_ATTEMPT)
            for task_id, status in [
                ("task_finished", TaskStatus.FINISHED),
                ("task_error", TaskStatus.ERROR),
                ("task_stopped", TaskStatus.STOPPED),
                ("task_pending", TaskStatus.PENDING),
            ]
        ]
        control_postgres_session.add_all(tasks)
        control_postgres_session.commit()
        control_postgres_session.add(EvaluationResult(org_id=TEST_ORG_ID, task=tasks[0].id, result={"score": 1.0}))
        control_postgres_session.commit()

        assert (await run_control.prepare_finalization()).kind == "deferred"
        tasks[3].status = TaskStatus.ERROR
        control_postgres_session.add(tasks[3])
        control_postgres_session.commit()
        assert (await run_control.prepare_finalization()).kind == "stopped"
        control_postgres_session.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.STOPPED

        benchmark.status = BenchmarkStatus.IN_PROGRESS
        tasks[2].status = TaskStatus.ERROR
        control_postgres_session.add_all([benchmark, tasks[2]])
        control_postgres_session.commit()
        run_control = cast(RunControl, bound_controls(benchmark, session=control_postgres_session)["run_control"])
        decision = await run_control.prepare_finalization()
        assert decision.kind == "score"
        assert decision.inputs is not None
        assert decision.inputs.evaluation_results == {
            "task_finished": {"score": 1.0},
            "task_error": None,
            "task_stopped": None,
            "task_pending": None,
        }

    @pytest.mark.description("Task failure records terminal status and structured error provenance through Control.")
    async def test_fail_task_records_status_and_provenance(
        self, contract: AgentContractRequest, control_postgres_session: Session, bound_controls: Any
    ) -> None:
        benchmark = make_benchmark(contract=contract)
        task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=benchmark.id,
            status=TaskStatus.IN_PROGRESS,
            started_at=_ACTIVE_ATTEMPT,
        )
        control_postgres_session.add_all([benchmark, task])
        control_postgres_session.commit()
        control = cast(TaskControl, bound_controls(benchmark, session=control_postgres_session)["task_control"])
        attempt = AttemptRef(org_id=TEST_ORG_ID, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)

        assert await control.fail(
            TaskFailureRequest(
                ref=control.ref,
                attempt=attempt,
                failure=_task_failure("agent failed"),
                expected_status=TaskStatus.IN_PROGRESS,
                retry_scheduled=False,
                failed_attempt_number=None,
            )
        )
        control_postgres_session.refresh(task)
        assert task.status == TaskStatus.ERROR
        error_result = control_postgres_session.exec(
            select(ErrorResult).where(ErrorResult.task == task.id).where(ErrorResult.org_id == TEST_ORG_ID)
        ).one()
        assert error_result.error_message == "agent failed"
        assert error_result.producer == "tracker"
        assert error_result.operation == "process_task"
        assert error_result.error_type == "RuntimeError"
        assert error_result.cause_code is None
        assert error_result.retry_scheduled is False
        assert error_result.failed_attempt_number is None

    @pytest.mark.description(
        "Search charges reported for two steps of one attempt are saved as two rows, and a repeated report adds none."
    )
    async def test_search_costs_keep_one_row_per_step(
        self, contract: AgentContractRequest, control_postgres_session: Session, bound_controls: Any
    ) -> None:
        benchmark = make_benchmark(contract=contract)
        task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=benchmark.id,
            status=TaskStatus.IN_PROGRESS,
            started_at=_ACTIVE_ATTEMPT,
        )
        control_postgres_session.add_all([benchmark, task])
        control_postgres_session.commit()
        control = cast(TaskControl, bound_controls(benchmark, session=control_postgres_session)["task_control"])
        attempt = AttemptRef(org_id=TEST_ORG_ID, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
        charges = [SearchCharge("exa", Decimal("0.007"))]

        for step in ("one", "two", "one"):
            await control.search_costs(
                TaskSearchCostsRequest(ref=control.ref, attempt=attempt, charges=charges, step_name=step)
            )

        control_postgres_session.expire_all()
        sources = control_postgres_session.exec(
            select(RunCost.source_id).where(RunCost.benchmark_id == benchmark.id)
        ).all()
        assert len(sources) == 2
        assert sorted(source.rsplit(":", 1)[-1] for source in sources) == ["one", "two"]

    @pytest.mark.description("Task failure leaves terminal tasks and their error history unchanged.")
    @pytest.mark.parametrize("status", [TaskStatus.FINISHED, TaskStatus.ERROR, TaskStatus.STOPPED])
    async def test_fail_task_preserves_terminal_tasks(
        self, contract: AgentContractRequest, control_postgres_session: Session, bound_controls: Any, status: TaskStatus
    ) -> None:
        benchmark = make_benchmark(contract=contract)
        task = Task(
            org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark.id, status=status, started_at=_ACTIVE_ATTEMPT
        )
        control_postgres_session.add_all([benchmark, task])
        control_postgres_session.commit()
        control = cast(TaskControl, bound_controls(benchmark, session=control_postgres_session)["task_control"])
        attempt = AttemptRef(org_id=TEST_ORG_ID, task_row_id=task.id, task_id=task.task_id, started_at=task.started_at)
        assert not await control.fail(
            TaskFailureRequest(
                ref=control.ref,
                attempt=attempt,
                failure=_task_failure("stale failure"),
                expected_status=None,
                retry_scheduled=False,
                failed_attempt_number=None,
            )
        )
        control_postgres_session.refresh(task)
        assert task.status == status
        assert control_postgres_session.exec(select(ErrorResult.id).where(ErrorResult.task == task.id)).first() is None

    @pytest.mark.description("Eval resume state rejects rows that are not active.")
    @pytest.mark.parametrize(
        "status", [TaskStatus.PENDING, TaskStatus.ERROR, TaskStatus.FINISHED, TaskStatus.STOPPED, TaskStatus.EVALUATING]
    )
    async def test_eval_resume_state_rejects_non_active_rows(
        self,
        contract: AgentContractRequest,
        control_postgres_session: Session,
        bound_controls: Any,
        status: TaskStatus,
    ) -> None:
        benchmark = make_benchmark(contract=contract)
        task = Task(
            org_id=TEST_ORG_ID,
            task_id="task_0",
            benchmark=benchmark.id,
            status=status,
            started_at=datetime(2026, 6, 30) if status == TaskStatus.EVALUATING else _ACTIVE_ATTEMPT,
        )
        control_postgres_session.add_all([benchmark, task])
        control_postgres_session.commit()
        control = cast(TaskControl, bound_controls(benchmark, session=control_postgres_session)["task_control"])
        token = uuid4()
        task.evaluation_phase_dispatch_id = control.ref.dispatch_id
        task.evaluation_phase_token = token
        control_postgres_session.add(task)
        control_postgres_session.commit()
        request = TaskResumeRequest(
            ref=control.ref,
            attempt=AttemptRef(
                org_id=TEST_ORG_ID, task_row_id=task.id, task_id=task.task_id, started_at=_ACTIVE_ATTEMPT
            ),
            phase=PhaseRef(token=token),
            state={"cursor": "next"},
        )
        if status == TaskStatus.EVALUATING:
            with pytest.raises(ExecutionAuthorityRevoked):
                await control.client.call("save_task_resume", request, TaskDecision)
        else:
            assert not (await control.client.call("save_task_resume", request, TaskDecision)).accepted
        control_postgres_session.refresh(task)
        assert task.eval_resume_state is None

    @pytest.mark.description("A stale task attempt cannot commit failure or change the active task.")
    async def test_fail_task_rejects_stale_attempt(
        self, contract: AgentContractRequest, control_postgres_session: Session, bound_controls: Any
    ) -> None:
        benchmark = make_benchmark(contract=contract)
        task = Task(
            org_id=TEST_ORG_ID,
            task_id="stale-task",
            benchmark=benchmark.id,
            status=TaskStatus.IN_PROGRESS,
            started_at=_ACTIVE_ATTEMPT,
        )
        control_postgres_session.add_all([benchmark, task])
        control_postgres_session.commit()
        control = cast(TaskControl, bound_controls(benchmark, session=control_postgres_session)["task_control"])
        stale = AttemptRef(
            org_id=TEST_ORG_ID, task_row_id=task.id, task_id=task.task_id, started_at=datetime(2000, 1, 1)
        )
        with pytest.raises(ExecutionAuthorityRevoked):
            await control.fail(
                TaskFailureRequest(
                    ref=control.ref,
                    attempt=stale,
                    failure=_task_failure("stale failure"),
                    expected_status=TaskStatus.IN_PROGRESS,
                    retry_scheduled=False,
                    failed_attempt_number=None,
                )
            )
        control_postgres_session.refresh(task)
        assert task.status == TaskStatus.IN_PROGRESS
        assert task.finished_at is None
        assert control_postgres_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).all() == []

    @pytest.mark.description("Finalization defers pending work, scores completed work, and stops interrupted runs.")
    async def test_finalize_benchmark_status(
        self, contract: AgentContractRequest, control_postgres_session: Session, bound_controls: Any
    ) -> None:
        benchmark = make_benchmark(contract=contract, concurrency=5)
        control_postgres_session.add(benchmark)
        control_postgres_session.commit()
        run_control = cast(RunControl, bound_controls(benchmark, session=control_postgres_session)["run_control"])
        task_ids = [f"task_{i}" for i in range(5)]
        snapshot = await run_control.read_run_state(task_ids, create_missing_tasks=True)
        assert len(snapshot.tasks) == len(task_ids)
        assert all(task.status == TaskStatus.PENDING for task in snapshot.tasks)
        assert (await run_control.prepare_finalization()).kind == "deferred"

        control_postgres_session.exec(
            update(Task)
            .where(col(Task.benchmark) == benchmark.id)
            .values(status=TaskStatus.FINISHED, finished_at=datetime.now(ZoneInfo("UTC")))
        )
        control_postgres_session.commit()
        assert (await run_control.prepare_finalization()).kind == "all_error"
        task = control_postgres_session.exec(select(Task).where(Task.benchmark == benchmark.id)).first()
        assert task is not None
        control_postgres_session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, result={"score": 1.0}))
        control_postgres_session.commit()
        decision = await run_control.prepare_finalization()
        assert decision.kind == "score" and decision.inputs is not None
        claim = await run_control.commit_final_score(decision.inputs.phase, decision.inputs.fingerprint, 1.0, {})
        assert claim.committed and claim.status == BenchmarkStatus.FINISHED
        control_postgres_session.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.FINISHED

        benchmark.status = BenchmarkStatus.IN_PROGRESS
        control_postgres_session.add(benchmark)
        control_postgres_session.exec(
            update(Task)
            .where(col(Task.task_id).in_(task_ids[:2]))
            .values(status=TaskStatus.STOPPED, finished_at=datetime.now(ZoneInfo("UTC")))
        )
        control_postgres_session.commit()
        run_control = cast(RunControl, bound_controls(benchmark, session=control_postgres_session)["run_control"])
        assert (await run_control.prepare_finalization()).kind == "stopped"
        control_postgres_session.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.STOPPED


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
