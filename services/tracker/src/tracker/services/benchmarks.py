from dataclasses import dataclass
from uuid import UUID

from sqlmodel import Session

from tracker.auth import RequestIdentity
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    Org,
    Task,
    benchmark_arguments_adapter,
)
from tracker.exceptions import TrackerServiceError
from tracker.repositories.benchmark import BenchmarkRepository
from tracker.types import RunExecutionRequest, StartBenchmarkRequest


@dataclass(frozen=True)
class BenchmarkConcurrencyUpdate:
    benchmark_id: UUID
    status: BenchmarkStatus
    concurrency: int


def start_benchmark_request_to_benchmark(
    request: StartBenchmarkRequest | RunExecutionRequest,
    run_starter: RequestIdentity,
    *,
    queue_pool_id: str | None = None,
) -> Benchmark:
    provider_secret_name = request.sandbox_provider_secret_name
    if request.environment == "aws" and (not request.sandbox_provider or not provider_secret_name):
        raise ValueError("Managed runs require a sandbox provider and provider secret name")

    return Benchmark(
        org_id=run_starter.org.id,
        name=request.benchmark_name,
        label=request.label,
        custom_benchmark_service=request.custom_benchmark_service,
        arguments=benchmark_arguments_adapter.validate_python(
            {
                "environment": request.environment,
                "properties": request.properties,
                "contract": request.contract,
                "concurrency": request.concurrency,
                "priority": request.priority,
                "queue_pool_id": queue_pool_id,
                "task_ids": request.task_ids,
                "slice_str": request.slice_str,
                "lambda_function": request.lambda_function,
                "dataset": request.dataset,
                "dataset_version": request.resolved_dataset_version
                if isinstance(request, RunExecutionRequest)
                else None,
                "sandbox_provider": request.sandbox_provider,
                "sandbox_provider_secret_name": provider_secret_name,
                "monitoring": request.monitoring,
                "monitoring_rules": request.monitoring_rules if isinstance(request, RunExecutionRequest) else None,
            },
        ),
        started_by_id=run_starter.access_key_id or run_starter.user_id,
        started_by_email=run_starter.email,
    )


def fetch_benchmark_row(
    benchmark_id: UUID,
    session: Session,
    org: Org,
    *,
    for_update: bool = False,
) -> Benchmark:
    benchmark_row = BenchmarkRepository(session).get_by_id(benchmark_id, for_update=for_update)
    if not benchmark_row:
        raise ValueError(f"Run with id {benchmark_id} not found")
    if benchmark_row.org_id != org.id:
        raise ValueError(f"Run {benchmark_id} does not belong to org {org.id}")
    return benchmark_row


def _fetch_locked_benchmark(benchmark_id: UUID, session: Session, org: Org) -> Benchmark:
    benchmark_row = BenchmarkRepository(session).lock(org.id, benchmark_id)
    if benchmark_row is None:
        raise ValueError(f"Run with id {benchmark_id} not found")
    return benchmark_row


def update_benchmark_concurrency(
    benchmark_id: UUID,
    concurrency: int,
    session: Session,
    org: Org,
) -> BenchmarkConcurrencyUpdate:
    benchmark_row = _fetch_locked_benchmark(benchmark_id, session, org)
    active = benchmark_row.status == BenchmarkStatus.IN_PROGRESS
    if active:
        benchmark_row.arguments = benchmark_row.arguments.model_copy(update={"concurrency": concurrency})
    result = BenchmarkConcurrencyUpdate(
        benchmark_id=benchmark_row.id,
        status=benchmark_row.status,
        concurrency=benchmark_row.arguments.concurrency,
    )
    if active:
        session.commit()
    return result


def update_benchmark_resume_arguments(
    benchmark_id: UUID,
    session: Session,
    org: Org,
    *,
    secrets: dict[str, str],
    concurrency: int | None,
    benchmark_url: str | None,
    lambda_function: str | None,
) -> Benchmark:
    benchmark_row = _fetch_locked_benchmark(benchmark_id, session, org)
    updates: dict[str, object] = {}
    if secrets:
        contract = benchmark_row.arguments.contract
        updates["contract"] = contract.model_copy(update={"secrets": {**contract.secrets, **secrets}})
    if concurrency is not None:
        updates["concurrency"] = concurrency
    if lambda_function is not None:
        updates["lambda_function"] = lambda_function
    if updates:
        benchmark_row.arguments = benchmark_row.arguments.model_copy(update=updates)
    if benchmark_url is not None:
        benchmark_row.custom_benchmark_service = benchmark_url
    return benchmark_row


def fetch_task_row(task_id: UUID, session: Session, org: Org) -> Task:
    task_row = BenchmarkRepository(session).get_task_by_id(task_id)
    if not task_row:
        raise TrackerServiceError(f"Task with id {task_id} not found")
    if task_row.org_id != org.id:
        raise TrackerServiceError(f"Task {task_id} does not belong to org {org.id}")
    return task_row
