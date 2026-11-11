from __future__ import annotations

from asyncio import gather
from collections.abc import AsyncGenerator, Callable
from typing import Any

from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import SetupTaskResponse
import pytest
from sqlalchemy.exc import OperationalError
from sqlmodel import Session, select

from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import copy_agent_to_benchmark, delete_from_s3, get_benchmark_contract_s3_key
from tracker.database.models import (
    Benchmark,
    Org,
    Task,
    TaskBreakdown,
)
from tracker.types import AgentContractRequest
from tracker.domain import BenchmarkStatus, TaskStatus
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.types import ManagedExecutionContext, RunExecutionRequest, StartBenchmarkRequest
from tracker.services.benchmarks import start_benchmark_request_to_benchmark
from tracker.services.runs.start import process_benchmark

_TASK_ID: str = "astropy__astropy-12907"
_TASK_IDS: list[str] = ["astropy__astropy-12907", "astropy__astropy-13033"]
_BENCHMARK: str = "swebench"

pytestmark = pytest.mark.usefixtures("live_deployment")


@pytest.fixture
def live_control_database(control_postgres_session: Session, live_aws_runtime: AWSRuntime) -> Session:
    org = control_postgres_session.get(Org, TEST_ORG_ID)
    assert org is not None
    org.storage_key = live_aws_runtime.resources.s3_bucket
    control_postgres_session.add(org)
    control_postgres_session.commit()
    return control_postgres_session


@pytest.fixture
async def frozen_contract_keys(live_aws_runtime: AWSRuntime) -> AsyncGenerator[set[str], None]:
    keys: set[str] = set()
    try:
        yield keys
    finally:
        for key in sorted(keys):
            await delete_from_s3(key, live_aws_runtime)


async def _create_benchmark(
    contract: AgentContractRequest,
    live_aws_runtime: AWSRuntime,
    frozen_contract_keys: set[str],
    session: Session,
    service_headers: dict[str, str],
    provider_secret_name: str,
    task_ids: list[str] | None = None,
    concurrency: int = 5,
) -> tuple[Benchmark, RunExecutionRequest]:
    request = StartBenchmarkRequest(
        benchmark_name=_BENCHMARK,
        contract=contract,
        concurrency=concurrency,
        task_ids=task_ids,
        properties=live_aws_runtime.resources,
        service_headers=service_headers,
        sandbox_provider="daytona",
        sandbox_provider_secret_name=provider_secret_name,
    )
    benchmark = start_benchmark_request_to_benchmark(request, RequestIdentity(org=Org(id=TEST_ORG_ID, name="default")))
    copied = await copy_agent_to_benchmark(
        str(benchmark.id),
        contract.name,
        live_aws_runtime,
    )
    if copied:
        frozen_contract_keys.add(get_benchmark_contract_s3_key(str(benchmark.id), contract.name))
    session.add(benchmark)
    session.commit()
    return benchmark, benchmark.managed_start_benchmark_request(service_headers)


def _execution_context(request: RunExecutionRequest, benchmark: Benchmark, task_ids: list[str]) -> dict[str, Any]:
    return ManagedExecutionContext(
        version=3,
        benchmark_id=benchmark.id,
        verified_task_ids=task_ids,
        start_benchmark_request=request,
    ).model_dump(mode="json")


def _task_rows(benchmark: Benchmark, session: Session) -> list[Task]:
    return list(session.exec(select(Task).where(Task.benchmark == benchmark.id)).all())


def _assert_no_task_errors(benchmark: Benchmark, session: Session) -> None:
    assert not BenchmarkRepository(session).task_failures(benchmark)


def _assert_task_breakdown_complete(task_breakdown: TaskBreakdown) -> None:
    assert task_breakdown.sandbox_build_duration is not None
    assert task_breakdown.agent_run_duration is not None
    assert task_breakdown.evaluation_run_duration is not None
    assert task_breakdown.sandbox_run_duration is not None


@pytest.mark.description("Live benchmark orchestration success, failure, and concurrency flows.")
class TestProcessBenchmark:
    @pytest.mark.description(
        (
            "Multiple tasks run concurrently and produce a complete benchmark result.\n"
            "\n"
            "Test cases:\n"
            "- Every task finishes without a captured task error.\n"
            "- Evaluation results, task breakdowns, and final evaluation exist for the completed benchmark."
        )
    )
    async def test_process_benchmark(
        self,
        contract: AgentContractRequest,
        live_control_database: Session,
        live_aws_runtime: AWSRuntime,
        frozen_contract_keys: set[str],
        service_headers: dict[str, str],
        daytona_secret_name: str,
        bound_controls: Any,
    ) -> None:
        benchmark, request = await _create_benchmark(
            contract,
            live_aws_runtime,
            frozen_contract_keys,
            live_control_database,
            service_headers,
            daytona_secret_name,
            task_ids=_TASK_IDS,
        )

        authority_kwargs = bound_controls(benchmark, session=live_control_database)
        await process_benchmark(
            execution_context_json=_execution_context(request, benchmark, _TASK_IDS), **authority_kwargs
        )

        live_control_database.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.FINISHED
        assert benchmark.error_message is None

        tasks = _task_rows(benchmark, live_control_database)
        assert len(tasks) == len(_TASK_IDS)
        assert all(task.status == TaskStatus.FINISHED for task in tasks)
        _assert_no_task_errors(benchmark, live_control_database)
        for task in tasks:
            task_breakdown = live_control_database.get(TaskBreakdown, task.task_breakdown)
            assert task_breakdown is not None
            _assert_task_breakdown_complete(task_breakdown)

        results = benchmark.fetch_evaluation_results(live_control_database)
        assert set(results.keys()) == set(_TASK_IDS)

        assert benchmark.final_evaluation is not None

    @pytest.mark.description(
        (
            "Benchmark-level errors set the benchmark status and error message.\n"
            "\n"
            "Test cases:\n"
            "- A database commit failure during benchmark startup marks the benchmark as ERROR.\n"
            "- The benchmark error message includes the underlying failure."
        )
    )
    async def test_process_benchmark_error(
        self,
        contract: AgentContractRequest,
        live_control_database: Session,
        live_aws_runtime: AWSRuntime,
        frozen_contract_keys: set[str],
        monkeypatch: pytest.MonkeyPatch,
        service_headers: dict[str, str],
        daytona_secret_name: str,
        bound_controls: Any,
    ) -> None:
        benchmark, request = await _create_benchmark(
            contract,
            live_aws_runtime,
            frozen_contract_keys,
            live_control_database,
            service_headers,
            daytona_secret_name,
            task_ids=[_TASK_ID],
        )

        original_commit = Session.commit
        commit_count = 0

        def failing_commit(self: Session) -> None:
            nonlocal commit_count
            commit_count += 1
            if commit_count == 1:
                raise OperationalError("COMMIT", None, RuntimeError("Simulated database error"))

            original_commit(self)

        authority_kwargs = bound_controls(benchmark, session=live_control_database)
        monkeypatch.setattr(Session, "commit", failing_commit)

        await process_benchmark(
            execution_context_json=_execution_context(request, benchmark, [_TASK_ID]), **authority_kwargs
        )

        live_control_database.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.ERROR
        assert "Simulated database error" in (benchmark.error_message or "")

    @pytest.mark.description(
        (
            "One task can fail setup while the other still completes through the real service path.\n"
            "\n"
            "Test cases:\n"
            "- The patched setup failure marks only the targeted task as ERROR.\n"
            "- The non-failing task still finishes, evaluates, and has no captured error."
        )
    )
    async def test_process_task_error(
        self,
        contract: AgentContractRequest,
        live_control_database: Session,
        live_aws_runtime: AWSRuntime,
        frozen_contract_keys: set[str],
        monkeypatch: pytest.MonkeyPatch,
        service_headers: dict[str, str],
        daytona_secret_name: str,
        bound_controls: Any,
    ) -> None:
        failing_task = "astropy__astropy-13033"
        task_ids = [_TASK_ID, failing_task]

        benchmark, request = await _create_benchmark(
            contract,
            live_aws_runtime,
            frozen_contract_keys,
            live_control_database,
            service_headers,
            daytona_secret_name,
            task_ids=task_ids,
        )

        original_setup_task = BenchmarkServiceClient.setup_task

        async def setup_task_with_failure(
            self: Any,
            task_id: str,
            instance_id: str,
            sandbox_provider: Any = None,
            on_message: Callable[[str], None] | None = None,
            **kwargs: Any,
        ) -> SetupTaskResponse:
            if task_id == failing_task:
                raise RuntimeError("Simulated setup failure")

            return await original_setup_task(
                self,
                task_id,
                instance_id,
                sandbox_provider=sandbox_provider,
                on_message=on_message,
                **kwargs,
            )

        monkeypatch.setattr(BenchmarkServiceClient, "setup_task", setup_task_with_failure)
        authority_kwargs = bound_controls(benchmark, session=live_control_database)

        await process_benchmark(
            execution_context_json=_execution_context(request, benchmark, task_ids), **authority_kwargs
        )

        live_control_database.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.FINISHED, benchmark.error_message
        assert benchmark.error_message is None

        error_tasks = live_control_database.exec(
            select(Task).where(Task.benchmark == benchmark.id).where(Task.status == TaskStatus.ERROR)
        ).all()
        assert len(error_tasks) == 1
        assert error_tasks[0].task_id == failing_task
        assert (
            "Simulated setup failure"
            in BenchmarkRepository(live_control_database).task_failures(benchmark)[failing_task][0]
        )

        finished_tasks = live_control_database.exec(
            select(Task).where(Task.benchmark == benchmark.id).where(Task.status == TaskStatus.FINISHED)
        ).all()
        assert len(finished_tasks) == 1
        assert finished_tasks[0].task_id == _TASK_ID

        results = benchmark.fetch_evaluation_results(live_control_database)
        assert len(results) == 1
        assert _TASK_ID in results
        assert failing_task not in results

    @pytest.mark.description(
        (
            "A run with no successful task results fails before final scoring.\n"
            "\n"
            "Test cases:\n"
            "- A missing declared output artifact marks the task as ERROR through the real sandbox path.\n"
            "- The benchmark is marked ERROR instead of attempting final scoring with only failed task inputs."
        )
    )
    async def test_process_benchmark_errors_when_all_tasks_fail_before_evaluation(
        self,
        contract: AgentContractRequest,
        live_control_database: Session,
        live_aws_runtime: AWSRuntime,
        frozen_contract_keys: set[str],
        service_headers: dict[str, str],
        daytona_secret_name: str,
        bound_controls: Any,
    ) -> None:
        failing_contract = contract.model_copy(update={"output_artifacts": ["missing-artifact.json"]})
        benchmark, request = await _create_benchmark(
            failing_contract,
            live_aws_runtime,
            frozen_contract_keys,
            live_control_database,
            service_headers,
            daytona_secret_name,
            task_ids=[_TASK_ID],
        )
        authority_kwargs = bound_controls(benchmark, session=live_control_database)

        await process_benchmark(
            execution_context_json=_execution_context(request, benchmark, [_TASK_ID]), **authority_kwargs
        )

        live_control_database.refresh(benchmark)
        assert benchmark.status == BenchmarkStatus.ERROR
        assert "No tasks were completed successfully" in (benchmark.error_message or "")
        assert "Required output artifact missing" in (benchmark.error_message or "")
        assert "Traceback" not in (benchmark.error_message or "")
        assert benchmark.final_evaluation is None

        tasks = _task_rows(benchmark, live_control_database)
        assert len(tasks) == 1
        assert tasks[0].status == TaskStatus.ERROR
        assert (
            "Required output artifact missing"
            in BenchmarkRepository(live_control_database).task_failures(benchmark)[_TASK_ID][0]
        )
        assert benchmark.fetch_evaluation_results(live_control_database) == {}

    @pytest.mark.description(
        (
            "Same task ID can run across two separate benchmarks concurrently without result collisions.\n"
            "\n"
            "Test cases:\n"
            "- Both benchmarks finish without benchmark or task errors.\n"
            "- Each benchmark has its own task row, evaluation result, and final evaluation."
        )
    )
    async def test_concurrent_benchmarks_same_task(
        self,
        contract: AgentContractRequest,
        live_control_database: Session,
        live_aws_runtime: AWSRuntime,
        frozen_contract_keys: set[str],
        service_headers: dict[str, str],
        daytona_secret_name: str,
        bound_controls: Any,
    ) -> None:
        benchmark_requests = [
            await _create_benchmark(
                contract,
                live_aws_runtime,
                frozen_contract_keys,
                live_control_database,
                service_headers,
                daytona_secret_name,
                task_ids=[_TASK_ID],
            )
            for _ in range(2)
        ]
        authority_by_benchmark = {
            benchmark.id: bound_controls(benchmark, session=live_control_database)
            for benchmark, _ in benchmark_requests
        }

        await gather(
            *[
                process_benchmark(
                    execution_context_json=_execution_context(request, benchmark, [_TASK_ID]),
                    **authority_by_benchmark[benchmark.id],
                )
                for benchmark, request in benchmark_requests
            ]
        )

        for benchmark, _ in benchmark_requests:
            live_control_database.refresh(benchmark)
            assert benchmark.status == BenchmarkStatus.FINISHED, (
                f"Benchmark {benchmark.id} error: {benchmark.error_message}"
            )
            assert benchmark.error_message is None
            assert benchmark.final_evaluation is not None

            tasks = _task_rows(benchmark, live_control_database)
            assert len(tasks) == 1
            assert tasks[0].task_id == _TASK_ID
            assert tasks[0].status == TaskStatus.FINISHED
            assert not BenchmarkRepository(live_control_database).task_failures(benchmark)

            results = benchmark.fetch_evaluation_results(live_control_database)
            assert set(results.keys()) == {_TASK_ID}
