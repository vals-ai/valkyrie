from datetime import datetime
from typing import Any
from uuid import UUID

from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.database.models import (
    AgentCausedExitReason,
    AgentContractRequest,
    Benchmark,
    AWSBenchmarkArguments,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    FailureCategory,
    Task,
    TaskStatus,
)


def make_benchmark(
    name: str = "swebench",
    *,
    status: BenchmarkStatus = BenchmarkStatus.IN_PROGRESS,
    org_id: UUID = TEST_ORG_ID,
    contract: AgentContractRequest | None = None,
    agent_name: str = "a",
    model: str | None = None,
    dataset: str | None = None,
    concurrency: int = 1,
    started_at: datetime | None = None,
    started_by_id: str | None = None,
    started_by_email: str | None = None,
    label: str | None = None,
    session: Session | None = None,
) -> Benchmark:
    benchmark = Benchmark(
        org_id=org_id,
        name=name,
        status=status,
        label=label,
        started_by_id=started_by_id,
        started_by_email=started_by_email,
        arguments=AWSBenchmarkArguments(
            contract=contract or AgentContractRequest(name=agent_name, model=model, install_cmd="i", run_cmd="r"),
            dataset=dataset,
            concurrency=concurrency,
            sandbox_provider="daytona",
            sandbox_provider_secret_name="test-provider-secret",
        ),
    )
    if started_at is not None:
        benchmark.started_at = started_at
    if session is not None:
        session.add(benchmark)
        session.commit()

    return benchmark


def make_task(
    benchmark: Benchmark,
    task_id: str,
    *,
    status: TaskStatus = TaskStatus.PENDING,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> Task:
    task = Task(
        org_id=benchmark.org_id,
        benchmark=benchmark.id,
        task_id=task_id,
        status=status,
        finished_at=finished_at,
    )
    if started_at is not None:
        task.started_at = started_at

    return task


def make_evaluation_result(
    task: Task,
    instance_id: str,
    result: dict[str, Any],
    created_at: datetime,
    *,
    exit_reason: AgentCausedExitReason | None = None,
) -> EvaluationResult:
    return EvaluationResult(
        org_id=task.org_id,
        task=task.id,
        instance_id=instance_id,
        result=result,
        agent_caused_exit_reason=exit_reason,
        created_at=created_at,
    )


def make_error_result(
    task: Task,
    error_message: str,
    created_at: datetime,
    *,
    producer: str | None = None,
    operation: str | None = None,
    error_type: str | None = None,
    cause_code: str | None = None,
    category: FailureCategory | None = None,
    retry_scheduled: bool = False,
    failed_attempt_number: int | None = None,
) -> ErrorResult:
    return ErrorResult(
        org_id=task.org_id,
        task=task.id,
        error_message=error_message,
        created_at=created_at,
        producer=producer,
        operation=operation,
        error_type=error_type,
        cause_code=cause_code,
        category=category,
        retry_scheduled=retry_scheduled,
        failed_attempt_number=failed_attempt_number,
    )
