from __future__ import annotations

from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from tracker.api.parsing import parse_csv
from tracker.api.dependencies import RunBenchmarkDependency, TrackedBenchmarkId
from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import CloudWatchBenchmarkLogLocations
from tracker.aws.resolver import resolve_run_metadata_aws_runtime
from tracker.aws.s3 import create_benchmark_url
from tracker.database.models import Org, TaskStatus
from tracker.database.scoping import assert_org
from tracker.repositories.benchmark import BenchmarkRepositoryDependency, TaskSort
from tracker.types import SingleBenchmarkResponse, TasksResponse, TaskSummary

router = APIRouter(prefix="/benchmarks")


@router.get(
    "/{benchmark_id}",
    response_model=SingleBenchmarkResponse,
    description="Return a run with its task counts and final score.",
)
async def get_single_benchmark(
    benchmark_id: TrackedBenchmarkId,
    request: Request,
    repository: BenchmarkRepositoryDependency,
    org: Org = Depends(get_current_org),
) -> SingleBenchmarkResponse:
    benchmark = assert_org(repository.get(org.id, benchmark_id), org)

    task_state_counts = repository.task_state_counts([benchmark.id]).get(benchmark.id, {})
    total = sum(task_state_counts.values())
    finished = (
        task_state_counts.get(TaskStatus.FINISHED, 0)
        + task_state_counts.get(TaskStatus.ERROR, 0)
        + task_state_counts.get(TaskStatus.STOPPED, 0)
    )

    cloudwatch_url: str | None = None
    s3_bucket_url: str | None = None
    storage_bucket: str | None = None
    if benchmark.arguments.environment == "local":
        cloudwatch_url = str(request.url_for("get_logs", benchmark_id=benchmark.id))
        s3_bucket_url = str(request.url_for("list_run_artifacts", benchmark_id=benchmark.id))
    else:
        aws_runtime = resolve_run_metadata_aws_runtime(
            properties=benchmark.arguments.properties,
            org=org,
        )
        if aws_runtime:
            aws_resources = aws_runtime.resources
            s3_bucket_url = create_benchmark_url(str(benchmark.id), aws_resources)
            storage_bucket = aws_resources.s3_bucket
            if aws_resources.log_group:
                cloudwatch_url = CloudWatchBenchmarkLogLocations(aws_resources).benchmark_location(str(benchmark.id))

    return SingleBenchmarkResponse(
        id=benchmark.id,
        name=benchmark.name,
        agent_name=benchmark.arguments.contract.name,
        model=benchmark.arguments.contract.model,
        executor_release_id=benchmark.executor_release_id,
        current_execution_release_id=benchmark.current_execution_release_id,
        executor_artifact_digest=benchmark.executor_artifact_digest,
        executor_protocol_version=benchmark.executor_protocol_version,
        started_at=benchmark.started_at,
        finished_at=benchmark.finished_at,
        status=benchmark.status,
        total_tasks=total,
        finished_tasks=finished,
        task_state_counts={status.value: count for status, count in task_state_counts.items()},
        started_by_email=benchmark.started_by_email,
        final_score=repository.final_score(benchmark),
        error_message=benchmark.error_message,
        cloudwatch_url=cloudwatch_url,
        s3_bucket_url=s3_bucket_url,
        storage_bucket=storage_bucket,
    )


@router.get(
    "/{benchmark_id}/tasks",
    response_model=TasksResponse,
    description=(
        "List a run's tasks a page at a time, filtered by status or task ID.\n"
        "\n"
        "sort=status desc puts errors first. The default order is newest started first.\n"
        "\n"
        "Returns 422 when `status` names an unknown task status."
    ),
)
def get_benchmark_tasks(
    benchmark: RunBenchmarkDependency,
    repository: BenchmarkRepositoryDependency,
    status: str = Query(default=""),
    task_id_search: str | None = None,
    sort: TaskSort = Query(default="started_at"),
    sort_dir: Literal["asc", "desc"] = Query(default="desc"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    org: Org = Depends(get_current_org),
) -> TasksResponse:
    try:
        statuses = parse_csv(status, TaskStatus)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    rows, total = repository.page_tasks(org.id, benchmark.id, statuses, task_id_search, sort, sort_dir, limit, offset)

    return TasksResponse(
        tasks=[
            TaskSummary(
                id=task.id,
                task_id=task.task_id,
                status=task.status,
                started_at=task.started_at,
                finished_at=task.finished_at,
                error_message=error_message if task.status == TaskStatus.ERROR else None,
                failure_category=category if task.status == TaskStatus.ERROR else None,
            )
            for task, error_message, category in rows
        ],
        total_count=total,
    )
